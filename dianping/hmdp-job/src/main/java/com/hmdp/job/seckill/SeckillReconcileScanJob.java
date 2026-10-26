package com.hmdp.job.seckill;

import com.hmdp.service.impl.SeckillReconcileScanService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

/**
 * 秒杀对账扫描 Job —— 检测 Redis 扣库存日志与订单表之间的不一致。
 *
 * <p>对账链路的两个 Job 是上下游关系：
 * <pre>{@code
 *   本 Job（SeckillReconcileScanJob）
 *     ↓ 定时扫描 → 发现不一致 → 写入 seckill_reconcile_task 表
 *   SeckillReconcileCompensationJob（对账补偿 Job）
 *     ↓ 读取 reconcile_task → 执行自动修复
 * }</pre></p>
 *
 * <p>扫描 Service 做双向对账（scanRecentRecords 内部分两步）：
 * <ul>
 *   <li><b>scanOrders</b>：查订单表中创建时间 ≥ scanDelayMinutes（默认 3 分钟前）的订单，
 *       逐条核对是否存在对应的 Redis 扣库存日志；有订单无日志 → ORDER_PRESENT_DEDUCT_MISSING</li>
 *   <li><b>scanDeductLogs</b>：查扣库存日志，逐条核对是否存在对应的订单；
 *       有日志无订单 → DEDUCT_PRESENT_ORDER_MISSING</li>
 * </ul>
 *
 * <p>scanDelayMinutes 的意义：给 MQ / Listener 留出建单时间，避免把"正在建单中"的正常情况误判为异常。</p>
 *
 * <p>与 {@link SeckillReconcileCompensationJob} 共用同一个
 * {@code seckill.reconcile.enabled} 总开关，保持对账链路整体可控。</p>
 */
@Slf4j
@Component
public class SeckillReconcileScanJob {

    /** 对账扫描服务 —— 双向核对库存日志与订单表、发现异常后写入 reconcile_task */
    @Resource
    private SeckillReconcileScanService seckillReconcileScanService;

    /**
     * 对账链路总开关 —— 与 CompensationJob 共享，关了扫描和补偿都停。
     * 默认 true，灰度 / 排查 / 废弃时可通过配置关闭。
     */
    @Value("${seckill.reconcile.enabled:true}")
    private boolean reconcileEnabled;

    /**
     * 执行对账扫描
     * <p>
     * fixedDelay = 15000：与 CompensationJob 同频，15 秒一轮。
     * 扫描是低频操作（每轮默认最多 scanBatchSize=100 条），没必要更密集。
     * </p>
     * <p>
     * scanRecentRecords 内部做双向扫描（先订单表 → 再扣库存日志），
     * 每条异常都先检查是否已存在 reconcile_task，防止重复写入。
     * </p>
     */
    @Scheduled(fixedDelay = 15000)
    public void scan() {
        // 对账总开关关闭 或 Service 未注入 → 跳过本轮
        if (!reconcileEnabled || seckillReconcileScanService == null) {
            return;
        }
        try {
            seckillReconcileScanService.scanRecentRecords();
        } catch (Exception e) {
            // Service 整体异常，打 ERROR 日志等待下一轮继续
            log.error("seckill reconcile scan job failed", e);
        }
    }
}