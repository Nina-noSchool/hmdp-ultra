package com.hmdp.service.impl;

import com.hmdp.entity.RollbackFailureLog;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.RollbackFailureLogMapper;
import com.hmdp.metrics.MetricsRecorder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.time.LocalDateTime;

/**
 * 库存回滚失败记录服务 —— "Redis 回滚失败"分支的落库 + 告警出口。
 *
 * <p>在此之前，回滚失败只有一行 error 日志，交对账引擎兜底；
 * 现在升级为三层留痕：① 落 tb_rollback_failure_log（来源 + 异常摘要，可审计）；
 * ② alert_status=PENDING 供告警系统扫描（当前为字段预留 + error 日志出口）；
 * ③ 后续由指标埋点把它计入 rollback_failure_total（批次2）。</p>
 *
 * <p>设计约束：本服务自身永不向外抛异常 —— 记录失败不能反过来破坏主流程，
 * 原始异常已在上层日志中保留；与"对账是最后防线"的既有分层保持一致。</p>
 */
@Slf4j
@Service
public class RollbackFailureService {

    /** 触发来源：秒杀主链路（发送失败回滚 / catch 分支回滚） */
    public static final String SOURCE_MAIN_CHAIN = "MAIN_CHAIN";

    /** 触发来源：消费端（OUT_OF_STOCK 回滚失败） */
    public static final String SOURCE_LISTENER = "LISTENER";

    /** 告警状态：待处理 */
    public static final String ALERT_PENDING = "PENDING";

    /** detail 字段长度上限（表结构 varchar(1024)，预留安全边界） */
    private static final int DETAIL_MAX_LENGTH = 1000;

    @Resource
    private RollbackFailureLogMapper rollbackFailureLogMapper;

    /** 业务指标登记处（批次2 ③）：回滚失败事件计数 */
    @Resource
    private MetricsRecorder metricsRecorder;

    /**
     * 记录一次回滚失败。
     *
     * @param source        触发来源（SOURCE_MAIN_CHAIN / SOURCE_LISTENER）
     * @param order         回滚涉及的订单（提供券/用户/订单号/traceId）
     * @param resultCode    结果码/摘要（如 REDIS_EXECUTE_ERROR）
     * @param retryAttempts 回滚尝试次数
     * @param cause         原始异常（可为 null）
     */
    public void record(String source, VoucherOrder order, String resultCode, int retryAttempts, Throwable cause) {
        try {
            metricsRecorder.incRollbackFailure();
            String detail = cause == null
                    ? null
                    : cause.getClass().getSimpleName() + ": " + cause.getMessage();
            if (detail != null && detail.length() > DETAIL_MAX_LENGTH) {
                detail = detail.substring(0, DETAIL_MAX_LENGTH);
            }
            LocalDateTime now = LocalDateTime.now();
            rollbackFailureLogMapper.insert(new RollbackFailureLog()
                    .setOrderId(order.getId())
                    .setUserId(order.getUserId())
                    .setVoucherId(order.getVoucherId())
                    .setTraceId(order.getTraceId())
                    .setSource(source)
                    .setResultCode(resultCode)
                    .setRetryAttempts(retryAttempts)
                    .setDetail(detail)
                    .setAlertStatus(ALERT_PENDING)
                    .setCreateTime(now)
                    .setUpdateTime(now));
            log.error("seckill rollback failure recorded, orderId={}, source={}, resultCode={}, reason={}",
                    order.getId(), source, resultCode, detail);
        } catch (Exception e) {
            // 记录失败只降级为日志，绝不向上抛（上层已保留原始异常日志）
            log.error("record rollback failure log error, orderId={}", order.getId(), e);
        }
    }
}
