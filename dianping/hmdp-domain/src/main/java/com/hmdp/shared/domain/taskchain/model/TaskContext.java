package com.hmdp.shared.domain.taskchain.model;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务链上下文
 * <p>
 * 贯穿整条任务链的数据载体，各节点通过它共享中间数据：
 * <ul>
 *   <li>input/output：链路的输入和最终输出</li>
 *   <li>attributes：节点间传递中间数据的 KV 存储（线程安全）</li>
 *   <li>success/errorMessage/exception：执行状态，节点失败时通过 markFailed 写入</li>
 *   <li>terminateChain：设为 true 后引擎会跳过后续所有节点</li>
 * </ul>
 * 并行执行时每个节点拿到的是 copy()，执行完后通过 mergeError/mergeData 合并回主 context。
 * </p>
 *
 * @param <I> 输入类型
 * @param <O> 输出类型
 */
@Getter
public class TaskContext<I, O> {

    private static final Logger log = LoggerFactory.getLogger(TaskContext.class);

    /**
     * 链路初始输入（比如商详链路的 shopId），由调用方在引擎执行前 set。
     */
    @Setter private I input;

    /**
     * 链路最终输出（比如商详链路的 ShopDetailAggregate），由最后一个节点写入。
     */
    @Setter private O output;

    /**
     * 节点间传递中间结果的 KV 存储。
     * 使用 ConcurrentHashMap 是因为并行节点的副本可能同时读写同一个 attributes 引用。
     */
    @Setter private Map<String, Object> attributes = new ConcurrentHashMap<>();

    /**
     * 属性写入者护栏（2026-10-29）：记录每个 key 最后一次写入的节点 ID，与 attributes
     * 一样在 copy() 时共享同一实例。约定是"并行节点只写各自不同的 key"——
     * 两个不同节点先后写同一 key 且值不同时，setAttribute 会打 WARN 显式暴露冲突，
     * 不再静默覆盖（最后一个写入者仍然获胜，行为不变，只是把隐式约定变成可观测的告警）。
     */
    private Map<String, String> attributeWriters = new ConcurrentHashMap<>();

    /** 写入者护栏视图（包级可见，供同包测试断言） */
    Map<String, String> attributeWriters() {
        return attributeWriters;
    }

    /**
     * 整条链路的成功标志。某个不可跳过节点失败时会被设为 false。
     */
    private boolean success = true;

    /**
     * 链路级熔断开关。
     * 设为 true 后，引擎在每次阶段循环开头会立即 break，后续所有阶段不再执行。
     * 不可跳过节点（skippable=false）失败时由引擎自动调用 markTerminate() 设置。
     */
    private boolean terminateChain = false;

    /**
     * 当前正在执行的节点 ID，引擎用来做日志追踪和超时定位。
     */
    @Setter private String currentNodeId;

    /**
     * 失败时的错误描述，由 markFailed 写入。
     */
    private String errorMessage;

    /**
     * 失败时的异常对象，由 markFailed 写入。
     */
    private Throwable exception;

    /**
     * 跳过当前节点的标志，节点内部可以调 markSkipCurrentNode 主动跳过自己。
     */
    @Setter private boolean skipCurrentNode = false;

    /**
     * 链路或阶段开始时间，引擎用来统计耗时。
     */
    @Setter private long startTime;

    /**
     * 链路或阶段结束时间，引擎用来统计耗时。
     */
    @Setter private long endTime;

    /**
     * 往 attributes 存中间结果。
     * value 为 null 时等价于删除该 key，方便节点用 setAttribute(key, null) 清理。
     * 写入者护栏：不同节点写同一 key 且值不同 → WARN（见 attributeWriters）。
     */
    public void setAttribute(String key, Object value) {
        if (value == null) {
            attributes.remove(key);
            return;
        }
        Object previous = attributes.put(key, value);
        String writer = currentNodeId == null ? "unknown" : currentNodeId;
        String previousWriter = attributeWriters.put(key, writer);
        if (previousWriter != null && !previousWriter.equals(writer)
                && previous != null && !previous.equals(value)) {
            log.warn("taskchain attribute conflict: key={} written by [{}] then [{}] with different values",
                    key, previousWriter, writer);
        }
    }

    /**
     * 从 attributes 取中间结果，调用方自己强转类型。
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key) {
        return (T) attributes.get(key);
    }

    /**
     * 标记链路失败：同时写 success 标志 + 错误信息 + 异常对象。
     * 引擎后续会根据 success 决定是否终止链路。
     */
    public void markFailed(String errorMessage, Throwable exception) {
        this.success = false;
        this.errorMessage = errorMessage;
        this.exception = exception;
    }

    /**
     * 一键熔断整条链路。
     * 引擎在每次循环开头检查 terminateChain，为 true 就 break，不再执行后续阶段。
     */
    public void markTerminate() {
        this.terminateChain = true;
    }

    /**
     * 让当前节点主动跳过自己（比如检测到重复执行、缓存命中时）。
     */
    public void markSkipCurrentNode() {
        this.skipCurrentNode = true;
    }

    /**
     * 拍快照：并行模式下为每个节点生成独立副本，避免多线程同时写同一个 context 导致数据竞争。
     * 注意：attributes 是浅拷贝（引用同一个 ConcurrentHashMap），因为并行节点只写各自不同的 key。
     */
    public TaskContext<I, O> copy() {
        TaskContext<I, O> copy = new TaskContext<>();
        copy.input = this.input;
        copy.output = this.output;
        copy.attributes = this.attributes;
        copy.attributeWriters = this.attributeWriters;
        copy.success = this.success;
        copy.terminateChain = this.terminateChain;
        copy.currentNodeId = this.currentNodeId;
        copy.errorMessage = this.errorMessage;
        copy.exception = this.exception;
        copy.skipCurrentNode = this.skipCurrentNode;
        copy.startTime = this.startTime;
        copy.endTime = this.endTime;
        return copy;
    }

    /**
     * 合并错误状态：并行节点全部跑完后，把失败状态从副本合并回主 context。
     * 规则：主 context 还成功时才覆盖（第一个失败的节点优先）；terminateChain 只要有一个副本终止就同步。
     */
    public void mergeError(TaskContext<I, O> other) {
        if (this.success && !other.success) {
            this.success = false;
            this.errorMessage = other.errorMessage;
            this.exception = other.exception;
        }
        if (other.terminateChain) {
            this.terminateChain = true;
        }
    }

    /**
     * 合并输出：并行节点的 output 谁最后写谁覆盖（通常只有 Assemble 节点会写 output）。
     */
    public void mergeData(TaskContext<I, O> other) {
        if (other.output != null) {
            this.output = other.output;
        }
    }
}