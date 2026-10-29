package com.hmdp.shared.domain.taskchain.service;

/**
 * 任务链监听器 SPI —— 引擎执行过程的可观测性扩展点（默认全部空实现，按需覆写）。
 *
 * <p>定位：核心引擎层。引擎在节点执行结束、阶段执行结束时回调监听器；
 * 监听器实现可以做结构化日志、Micrometer Timer、慢节点告警等，
 * 引擎自身保持零观测依赖。</p>
 *
 * <p>接入方式：注册为 Spring Bean 后由 SharedEngineAutoConfiguration 收集，
 * 注入 TaskDispatcher / PhaseExecutionEngine。监听器抛出的异常会被引擎吞掉
 * 并降级为 WARN 日志——可观测性组件故障绝不能影响业务执行。</p>
 */
public interface TaskChainListener {

    /**
     * 单个节点执行结束（无论成功失败，含串行与并行）。
     *
     * @param phase   所属阶段名（ProcessPhase.name()）
     * @param nodeId  节点 ID（@TaskNode.nodeId()）
     * @param taskType 任务类型（IO_BOUND / CPU_BOUND）
     * @param costMs  节点执行耗时（毫秒）
     * @param success 执行是否成功（context.isSuccess()）
     * @param error   失败时的错误描述（成功时为 null）
     */
    default void onNodeEnd(String phase, String nodeId, String taskType, long costMs, boolean success, String error) {
    }

    /**
     * 单个阶段执行结束（含失败/超时）。
     *
     * @param phase   阶段名（ProcessPhase.name()）
     * @param costMs  阶段执行耗时（毫秒）
     * @param success 阶段是否成功
     * @param error   失败时的错误描述（成功时为 null）
     */
    default void onPhaseEnd(String phase, long costMs, boolean success, String error) {
    }
}
