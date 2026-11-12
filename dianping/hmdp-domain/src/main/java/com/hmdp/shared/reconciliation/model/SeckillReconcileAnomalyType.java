package com.hmdp.shared.reconciliation.model;

public enum SeckillReconcileAnomalyType {
    ORDER_PRESENT_DEDUCT_MISSING,
    DEDUCT_PRESENT_ORDER_MISSING,
    RESTORE_EXPECTED_MISSING,
    DUPLICATE_OR_CONFLICT_LOG,
    /**
     * Redis 有 DEDUCT 追踪日志，但 DB 侧订单与对账日志均无（"Redis 已扣、DB 无痕"的幽灵预占）。
     * 由 trace 日志扫描方向发现（seckill.reconcile.trace-scan-enabled，默认关）；
     * 只发现不自动补偿，一律转人工（pending / NOT_NEEDED）。
     */
    REDIS_DEDUCT_PRESENT_DB_MISSING
}
