package com.hmdp.job.seckill;

import com.hmdp.service.impl.SeckillReconcileCompensationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

/**
 * 秒杀对账补偿 Job —— 对账发现异常后自动执行修复动作。
 *
 * <p>与前面三个 Job 的定位差异：
 * <ul>
 *   <li>MqEventRetryJob / RecoveryJob / VerifyJob —— <b>建单链路内的兜底</b>，目标是让订单最终建成</li>
 *   <li>本 Job —— <b>建单完成后的对账补偿</b>，由对账系统（SeckillReconcileJob）检测出库存 / 订单不一致后写入
 *       {@code seckill_reconcile_task} 表，本 Job 定时扫描并执行补偿（回滚库存 或 补发订单）</li>
 * </ul></p>
 *
 * <p>补偿类型举例（Service 内部实现）：
 * <ul>
 *   <li>DEDUCT_PRESENT_ORDER_MISSING：Redis 扣库存日志存在，但订单表找不到订单 —— 尝试自动重建订单</li>
 *   <li>（其他异常类型可按需扩展，Service 对不支持的异常类型直接标记 FAILED）</li>
 * </ul></p>
 *
 * <p>注意：Service 类头部有 {@code // TODO 这段代码废弃掉}，
 * 如果后续确实废弃，记得同步关闭 {@code seckill.reconcile.enabled=false} 停掉本 Job 的调度。</p>
 */
@Slf4j
@Component
public class SeckillReconcileCompensationJob {

    /** 对账补偿服务 —— 扫描补偿任务、执行自动修复、维护补偿状态 */
    @Resource
    private SeckillReconcileCompensationService seckillReconcileCompensationService;

    /**
     * Job 总开关 —— 允许通过配置禁用整个对账补偿链路。
     * <p>
     * 用途：灰度发布、故障排查、或代码废弃后快速停掉定时任务，避免 DB 空扫。
     * 默认 true，关闭后 compensate() 首行即 return，不再执行任何逻辑。
     * </p>
     */
    @Value("${seckill.reconcile.enabled:true}")
    private boolean reconcileEnabled;

    /**
     * 执行对账补偿
     * <p>
     * fixedDelay = 15000：上一轮 compensate() 结束后等 15 秒再跑下一轮。
     * 对比前三个 Job 的 5 秒，本 Job 间隔更长，因为对账补偿是低频场景（对账 Job 通常比建单 Job 更慢发现问题），
     * 没必要高频轮询给 DB 加负担。
     * </p>
     * <p>
     * 双重前置检查：reconcileEnabled=false（总开关关了）或 Service 未注入 → 直接 return。
     * </p>
     */
    @Scheduled(fixedDelay = 15000)
    public void compensate() {
        // 总开关关闭 或 Service 未注入 → 跳过本轮
        if (!reconcileEnabled || seckillReconcileCompensationService == null) {
            return;
        }
        try {
            seckillReconcileCompensationService.processDueTasks();
        } catch (Exception e) {
            // Service 整体异常，打 ERROR 日志等待下一轮继续
            log.error("seckill reconcile compensation job failed", e);
        }
    }
}