package com.hmdp.job.seckill;

import com.hmdp.service.impl.OrderCreateRecoveryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

/**
 * 秒杀订单创建恢复 Job — 兜底处理 MQ 重试耗尽后仍未成功建单的秒杀请求。
 *
 * <p>触发时机：
 * <pre>{@code
 *   用户秒杀下单 → 发 MQ → Listener 异步建单
 *                                    ↓ 失败（批量消费不依赖 MQ 重试，见 README 取舍 #2）
 *                            写入 order_create_recovery_task 表
 *                                    ↓
 *                            本 Job 每 5 秒捞取并自动恢复
 *                            （每轮先回收卡在 PROCESSING 超过 5 分钟的任务）
 * }</pre></p>
 *
 * <p>与 {@link MqEventRetryJob} 的区别：
 * <ul>
 *   <li>MqEventRetryJob 是 <b>RocketMQ Broker 层面的重发</b>（消息没进到 Broker）</li>
 *   <li>本 Job 是 <b>业务层面的兜底</b>（消息进了 Broker 但消费端处理失败，监听器转交 recovery_task）</li>
 * </ul></p>
 *
 * <p>恢复策略：Service 内部按 retryCount 判定是否继续自动重试，
 * 超过 maxRetryCount 则升级为人工审核。</p>
 */
@Slf4j
@Component
public class OrderCreateRecoveryJob {

    /** 卡在 PROCESSING 超过该分钟数视为崩溃残留，重置回 PENDING 重新驱动 */
    private static final int STUCK_PROCESSING_MINUTES = 5;

    /** 订单恢复服务 —— 扫描到期任务、自动重建订单、维护重试计数 */
    @Resource
    private OrderCreateRecoveryService orderCreateRecoveryService;

    /**
     * 执行订单恢复任务
     * <p>
     * fixedDelay = 5000 含义：上一次 run() 执行完毕后，等待 5 秒再执行下一次。
     * 与 cron 固定频率不同，fixedDelay 是 <b>上一轮结束才计算下一轮开始时间</b>，
     * 适合这种需要串行处理、避免重叠的业务恢复场景。
     * </p>
     * <p>
     * 整段 try-catch：防止 Service 内部异常直接炸掉定时任务线程，打 ERROR 日志后等待下一轮继续。
     * 注意这里只包一层 try-catch，没有像 MqEventRetryJob 那样单条 try-catch，
     * 因为具体任务的逐条错误处理在 Service.processDueTasks() 内部。
     * </p>
     */
    @Scheduled(fixedDelay = 5000)
    public void run() {
        // 防御性空值检查 —— 启动阶段 Bean 未注入完成时直接跳过
        if (orderCreateRecoveryService == null) {
            return;
        }
        try {
            int reset = orderCreateRecoveryService.resetStuckProcessingTasks(STUCK_PROCESSING_MINUTES);
            if (reset > 0) {
                log.warn("reset {} stuck PROCESSING recovery tasks", reset);
            }
            orderCreateRecoveryService.processDueTasks();
        } catch (Exception e) {
            // Service 整体异常（如 DB 不可用），打 ERROR 日志等待下一轮重试
            log.error("order create recovery job failed", e);
        }
    }
}