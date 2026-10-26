package com.hmdp.job.seckill;

import com.hmdp.service.impl.OrderCreateVerificationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

/**
 * 秒杀订单创建校验 Job —— 对账兜底，检测"扣了库存但没建单"的不一致。
 *
 * <p>检测时机与场景：
 * <pre>{@code
 *   用户秒杀下单
 *     ↓
 *   Redis 扣库存成功 ✅
 *     ↓
 *   发 MQ 建单 ──→ Listener 建单 ──→ 成功 ✅
 *                                     ↓ 失败（转 recovery_task）
 *                                     ↓
 *                        两个兜底 Job 同时介入：
 *                         ├─ OrderCreateRecoveryJob（主动重试建单）
 *                         └─ 本 Job（查询订单表确认是否真的没建单，防止误判）
 * }</pre></p>
 *
 * <p>与 {@link OrderCreateRecoveryJob} 的区别：
 * <ul>
 *   <li>RecoveryJob 是 <b>主动修复</b>——假设订单没建成，调 Service 重建订单</li>
 *   <li>本 Job 是 <b>被动校验</b>——先查订单表，确认订单真的不存在才触发恢复，避免重复建单</li>
 * </ul></p>
 *
 * <p>两者互补：RecoveryJob 依赖监听器写入的 recovery_task（不等 MQ 重试），
 * 本 Job 依赖定时查询订单表是否存在，覆盖 RecoveryJob 没覆盖到的边缘场景；
 * 本 Job 每轮还会回收卡在 VERIFYING 超过 5 分钟的任务（进程崩溃残留）。</p>
 */
@Slf4j
@Component
public class OrderCreateVerifyJob {

    /** 卡在 VERIFYING 超过该分钟数视为崩溃残留，重置回 PENDING 重新驱动 */
    private static final int STUCK_VERIFYING_MINUTES = 5;

    /** 订单创建校验服务 —— 扫描到期校验任务、查询订单表、触发恢复或标记结果 */
    @Resource
    private OrderCreateVerificationService orderCreateVerificationService;

    /**
     * 执行订单创建校验
     * <p>
     * fixedDelay = 5000 含义：上一轮 verify() 执行完毕后等 5 秒再跑下一轮，
     * 与 RecoveryJob 保持相同节奏，两个 Job 并行巡检、互不阻塞。
     * </p>
     * <p>
     * 外层 try-catch：兜底捕获 Service 整体异常（如 DB 不可用），
     * 单条校验任务的错误处理下沉到 Service.verifyDueTasks() 内部。
     * </p>
     */
    @Scheduled(fixedDelay = 5000)
    public void verify() {
        // 防御性空值检查 —— 启动阶段 Bean 未注入完成时直接跳过
        if (orderCreateVerificationService == null) {
            return;
        }
        try {
            int reset = orderCreateVerificationService.resetStuckVerifyingTasks(STUCK_VERIFYING_MINUTES);
            if (reset > 0) {
                log.warn("reset {} stuck VERIFYING verify tasks", reset);
            }
            orderCreateVerificationService.verifyDueTasks();
        } catch (Exception e) {
            // Service 整体异常，打 ERROR 日志等待下一轮继续
            log.error("order create verification job failed", e);
        }
    }
}