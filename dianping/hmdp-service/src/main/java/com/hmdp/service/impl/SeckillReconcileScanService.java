package com.hmdp.service.impl;

import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.SeckillReconcileTask;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherReconcileLog;
import com.hmdp.mapper.SeckillReconcileTaskMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherReconcileLogMapper;
import com.hmdp.metrics.MetricsRecorder;
import com.hmdp.shared.reconciliation.model.ReconcileLogType;
import com.hmdp.shared.reconciliation.model.SeckillReconcileAnomalyType;
import com.hmdp.shared.reconciliation.model.SeckillReconcileCompensationStatus;
import com.hmdp.shared.reconciliation.model.SeckillReconcileTaskStatus;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class SeckillReconcileScanService {

    /** Redis 追踪日志 key 模式：seckill:trace:log:{voucherId}（与 seckill_plus.lua 写入约定一致） */
    private static final String TRACE_LOG_KEY_PATTERN = "seckill:trace:log:*";
    private static final String LOG_TYPE_DEDUCT = "DEDUCT";
    private static final String LOG_TYPE_RESTORE = "RESTORE";

    private final SeckillReconcileTaskMapper taskMapper;
    private final VoucherOrderMapper voucherOrderMapper;
    private final VoucherReconcileLogMapper voucherReconcileLogMapper;
    private final MetricsRecorder metricsRecorder;
    private final StringRedisTemplate stringRedisTemplate;
    private final int scanBatchSize;
    private final int scanDelayMinutes;
    private final int compensationMaxRetryCount;
    private final boolean autoCompensationEnabled;
    private final boolean traceScanEnabled;

    @Autowired
    public SeckillReconcileScanService(SeckillReconcileTaskMapper taskMapper,
                                       VoucherOrderMapper voucherOrderMapper,
                                       VoucherReconcileLogMapper voucherReconcileLogMapper,
                                       MetricsRecorder metricsRecorder,
                                       StringRedisTemplate stringRedisTemplate,
                                       @Value("${seckill.reconcile.scan-batch-size:100}") int scanBatchSize,
                                       @Value("${seckill.reconcile.scan-delay-minutes:3}") int scanDelayMinutes,
                                       @Value("${seckill.reconcile.compensation-max-retry-count:5}") int compensationMaxRetryCount,
                                       @Value("${seckill.reconcile.auto-compensation-enabled:false}") boolean autoCompensationEnabled,
                                       @Value("${seckill.reconcile.trace-scan-enabled:false}") boolean traceScanEnabled) {
        this.taskMapper = taskMapper;
        this.voucherOrderMapper = voucherOrderMapper;
        this.voucherReconcileLogMapper = voucherReconcileLogMapper;
        this.metricsRecorder = metricsRecorder;
        this.stringRedisTemplate = stringRedisTemplate;
        this.scanBatchSize = scanBatchSize;
        this.scanDelayMinutes = scanDelayMinutes;
        this.compensationMaxRetryCount = compensationMaxRetryCount;
        this.autoCompensationEnabled = autoCompensationEnabled;
        this.traceScanEnabled = traceScanEnabled;
    }

    /**
     * 对账扫描入口 —— 三个视角交叉比对，找出不一致的记录并生成 {@link SeckillReconcileTask} 待处理。
     *
     * <p>扫描方向是不对称的，严重程度不同：
     * <ul>
     *   <li>{@link #scanOrders()} —— 以订单表为主，查订单存在但 DEDUCT 日志缺失（"订单在但库存没扣"）</li>
     *   <li>{@link #scanDeductLogs()} —— 以 DEDUCT 日志为主，查日志存在但订单缺失（"库存扣了但订单没建"）</li>
     *   <li>{@link #scanTraceLogs()} —— 以 Redis 追踪日志为主，查"Redis 已扣、DB 侧完全无痕"的幽灵预占。
     *       前两个方向都以 DB 表为锚，覆盖不了"Redis 已动、DB 未留痕"这个中间态（压测归档的系统性盲区）；
     *       本方向补上它，只发现不修复，独立开关 {@code seckill.reconcile.trace-scan-enabled} 控制，默认关</li>
     * </ul></p>
     *
     * <p>所有方向都带 {@code scanDelayMinutes} 延迟窗口（默认 3 分钟），
     * 给异步链路留足时间（MQ 消费 → DB 事务 → 日志落库）再对账，避免把"正在跑"的短暂不一致误判成异常。</p>
     */
    public void scanRecentRecords() {
        scanOrders();
        scanDeductLogs();
        scanTraceLogs();
    }

    private void scanOrders() {
        QueryWrapper<VoucherOrder> wrapper = new QueryWrapper<>();
        // 取"scanDelayMinutes 之前"的订单 —— 用 <= 而不是 >=，
        // 因为我们要找的是"等异步链路跑完还没对齐"的记录，最近几秒的订单 MQ 消费者可能还在处理中
        wrapper.le("create_time", LocalDateTime.now().minusMinutes(scanDelayMinutes))
                .last("limit " + scanBatchSize);
        List<VoucherOrder> orders = voucherOrderMapper.selectList(wrapper);
        for (VoucherOrder order : orders) {
            // N+1 查询：每条订单单独查一次 DEDUCT 日志。对账是离线低频任务（Job 每 5 分钟跑一次，
            // 每次最多 scanBatchSize=100 条），DB 查询量可控，没必要为了 JOIN/IN 把代码复杂化
            VoucherReconcileLog deductLog = voucherReconcileLogMapper.selectOne(new QueryWrapper<VoucherReconcileLog>()
                    .eq("order_id", order.getId())
                    .eq("log_type", ReconcileLogType.DEDUCT)
                    .last("limit 1"));
            if (deductLog == null) {
                // 异常：订单在但 DEDUCT 日志缺失 → 说明消费者可能 insert 成功但写日志时炸了，
                // DB 库存大概率也没扣。这里只建 task 不直接补偿 —— createTaskIfAbsent 内部会判断
                // autoCompensationEnabled 开关，决定是 pending 等人工还是 pendingCompensation 等自动
                createTaskIfAbsent(SeckillReconcileAnomalyType.ORDER_PRESENT_DEDUCT_MISSING,
                        order.getId(),
                        order.getUserId(),
                        order.getVoucherId(),
                        order.getTraceId(),
                        "order exists but deduct log missing");
            }
        }
    }

    private void scanDeductLogs() {
        QueryWrapper<VoucherReconcileLog> wrapper = new QueryWrapper<>();
        // 以 DEDUCT 日志为主视角 —— 前面 scanOrders 是反方向（订单表为主），
        // 两个方法合起来才是完整的双向对账：A 在 B 不在、B 在 A 不在都能捕获
        wrapper.eq("log_type", ReconcileLogType.DEDUCT)
                .le("create_time", LocalDateTime.now().minusMinutes(scanDelayMinutes))
                .last("limit " + scanBatchSize);
        List<VoucherReconcileLog> logs = voucherReconcileLogMapper.selectList(wrapper);
        for (VoucherReconcileLog log : logs) {
            // 这里比 scanOrders 的反向查询更简单 —— 直接 selectById，主键索引 O(1)
            VoucherOrder order = voucherOrderMapper.selectById(log.getOrderId());
            if (order == null) {
                // ⚠️ 严重异常：DEDUCT 日志存在意味着 DB 库存已经真扣了，但订单丢了！
                // 用户付不了款、退不回库存，属于数据层面的"库存黑洞"。
                // createTaskIfAbsent 内部对这个 anomalyType 有特殊处理：
                // autoCompensationEnabled=true 时走 pendingCompensation（自动补单），
                // 否则走 pending（等人工） —— 默认关，保守策略避免自动补偿再出错
                createTaskIfAbsent(SeckillReconcileAnomalyType.DEDUCT_PRESENT_ORDER_MISSING,
                        log.getOrderId(),
                        log.getUserId(),
                        log.getVoucherId(),
                        log.getTraceId(),
                        "deduct log exists but order missing");
            }
        }
    }

    /**
     * 第三扫描方向：以 Redis 追踪日志（seckill:trace:log:{voucherId}）为主视角，
     * 找出"Redis 已 DEDUCT、但 DB 侧订单与对账日志均无"的幽灵预占。
     *
     * <p>Redis 读取纪律（2 天 TTL 下追踪日志可能累积大量 field，绝不能全量读）：
     * <ul>
     *   <li>key 发现用 <b>SCAN</b>（match + count），禁止 KEYS；</li>
     *   <li>每个 key 用 <b>HSCAN 游标分批</b>读取 field，禁止 HGETALL / opsForHash().entries()；</li>
     *   <li>单轮处理的 field 总量受 {@code scanBatchSize} 预算限制，超预算留到下一轮（无游标状态，
     *       下一轮从头扫——由 createTaskIfAbsent 的查重兜住重复建单，代价是已扫过的 field 会重读，
     *       换来实现无状态、可随轮次恢复）。</li>
     * </ul></p>
     *
     * <p>判定流程（每条 field）：
     * <ol>
     *   <li>JSON 解析失败 → 跳过（记 warn，不让单条脏数据打断整轮扫描）；</li>
     *   <li>logType != DEDUCT（含回滚覆盖写成的 RESTORE）→ 跳过；</li>
     *   <li>ts 晚于 now - scanDelayMinutes → 在途，跳过；</li>
     *   <li>按 traceId 查 DB 已有 RESTORE 对账日志 → 已回滚，跳过（避免把已回滚报成异常）；</li>
     *   <li>orderId 能查到订单 → DB 侧正常，跳过；</li>
     *   <li>按 traceId 查 DB 有任何对账日志 → DB 有痕，跳过；</li>
     *   <li>全部未命中 → 生成 REDIS_DEDUCT_PRESENT_DB_MISSING 工单（pending / 人工）。</li>
     * </ol></p>
     *
     * <p>查重定位键：这类异常 DB 侧没有订单，orderId 不能作为定位键，
     * 因此建单时显式传 orderId=null，让 {@link #countActiveTask} 走 traceId 分支；
     * 真实 orderId 只写进 anomalyDetail 供人工排查。</p>
     */
    private void scanTraceLogs() {
        if (!traceScanEnabled) {
            return;
        }
        int budget = scanBatchSize;
        try (Cursor<String> keys = stringRedisTemplate.scan(
                ScanOptions.scanOptions().match(TRACE_LOG_KEY_PATTERN).count(scanBatchSize).build())) {
            while (keys.hasNext() && budget > 0) {
                String traceLogKey = keys.next();
                budget = scanTraceLogKey(traceLogKey, budget);
            }
        } catch (Exception e) {
            // Redis 整体异常（连接断开等）：本轮扫描放弃，等下一轮；不影响前两个 DB 方向的结果。
            // 心跳不打卡——idle gauge 持续增大，告警链路据此发现"扫描器死亡"
            log.warn("trace log scan aborted, will retry next round", e);
            return;
        }
        // 本轮完整跑完（单 key 失败被 scanTraceLogKey 内部吸收，不算整轮失败）→ 心跳打卡，
        // hmdp.reconcile.trace.scan.idle.seconds 归零；告警规则盯它的持续增大
        metricsRecorder.markTraceScanSuccess();
    }

    /**
     * 扫描一个追踪日志 Hash，返回扣减后的本轮剩余预算（每读一条 field 扣 1）。
     */
    private int scanTraceLogKey(String traceLogKey, int budget) {
        long windowBefore = System.currentTimeMillis() - scanDelayMinutes * 60_000L;
        try (Cursor<Map.Entry<Object, Object>> entries = stringRedisTemplate.opsForHash()
                .scan(traceLogKey, ScanOptions.scanOptions().count(scanBatchSize).build())) {
            while (entries.hasNext() && budget > 0) {
                Map.Entry<Object, Object> field = entries.next();
                budget--;
                checkTraceEntry(traceLogKey, field.getValue(), windowBefore);
            }
        } catch (Exception e) {
            // 单 key 异常：记 warn 跳过该 key，继续扫其他 key（不让一个坏 key 拖垮整轮）
            log.warn("trace log key scan failed: {}", traceLogKey, e);
        }
        return budget;
    }

    private void checkTraceEntry(String traceLogKey, Object rawValue, long windowBefore) {
        TraceLogEntry entry = parseEntry(rawValue);
        if (entry == null) {
            return;
        }
        // logType != DEDUCT（含回滚脚本覆盖同一 field 写成的 RESTORE）→ 跳过
        if (!LOG_TYPE_DEDUCT.equals(entry.getLogType())) {
            return;
        }
        // 在途窗口内 → 给异步链路留时间，跳过
        if (entry.getTsLong() > windowBefore) {
            return;
        }
        // DB 已有 RESTORE 对账日志 → 已回滚（Redis 追踪可能还没被覆盖写），跳过
        VoucherReconcileLog dbRestore = voucherReconcileLogMapper.selectOne(new QueryWrapper<VoucherReconcileLog>()
                .eq("trace_id", entry.getTraceId())
                .eq("log_type", ReconcileLogType.RESTORE)
                .last("limit 1"));
        if (dbRestore != null) {
            return;
        }
        // orderId 查得到订单 → DB 侧正常，跳过
        Long orderId = parseId(entry.getOrderId());
        if (orderId != null && voucherOrderMapper.selectById(orderId) != null) {
            return;
        }
        // 按 traceId 查 DB 有任何对账日志 → DB 有痕（DEDUCT/RESTORE 任一），跳过
        Number dbTraceCount = voucherReconcileLogMapper.selectCount(new QueryWrapper<VoucherReconcileLog>()
                .eq("trace_id", entry.getTraceId()));
        if (dbTraceCount != null && dbTraceCount.longValue() > 0) {
            return;
        }
        // Redis 有 DEDUCT、DB 侧完全无痕 → 幽灵预占。
        // orderId 显式传 null：DB 没有订单时它不能作为查重定位键（走 traceId 分支），
        // 真实 orderId 进 detail 供人工排查；一律 pending 转人工，不做自动补偿。
        // 证据随工单走：完整追踪快照（beforeQty/afterQty/ts）拼进 detail——
        // 追踪日志 TTL 只有 2 天，人工介入时 Redis 侧可能已过期，工单是唯一持久凭证
        log.info("redis deduct trace without db record: key={} traceId={} orderId={}",
                traceLogKey, entry.getTraceId(), entry.getOrderId());
        String rawSnapshot = rawValue instanceof String s ? s : String.valueOf(rawValue);
        createTaskIfAbsent(SeckillReconcileAnomalyType.REDIS_DEDUCT_PRESENT_DB_MISSING,
                null,
                entry.getUserId(),
                entry.getVoucherId(),
                entry.getTraceId(),
                "redis deduct trace present but no db order/reconcile log; orderId=" + entry.getOrderId()
                        + "; traceSnapshot=" + rawSnapshot);
    }

    /**
     * 解析追踪日志 field 的 JSON 值；解析失败 / 关键字段缺失 / ts 非数字 → 返回 null 跳过。
     */
    private TraceLogEntry parseEntry(Object rawValue) {
        if (!(rawValue instanceof String json) || json.isEmpty()) {
            return null;
        }
        try {
            TraceLogEntry entry = JSONUtil.toBean(json, TraceLogEntry.class);
            if (entry == null
                    || StrUtil.isBlank(entry.getTraceId())
                    || !NumberUtil.isLong(entry.getTs())) {
                return null;
            }
            entry.setTsLong(Long.parseLong(entry.getTs()));
            return entry;
        } catch (Exception e) {
            log.warn("skip malformed trace log entry: {}", StrUtil.subPre(json, 200), e);
            return null;
        }
    }

    private Long parseId(String id) {
        if (StrUtil.isBlank(id) || !NumberUtil.isLong(id)) {
            return null;
        }
        return Long.parseLong(id);
    }

    /**
     * 追踪日志 field 的 JSON 结构（与 seckill_plus.lua 的 cjson.encode 约定一致）。
     * orderId/userId/voucherId/ts 在 Lua 侧是字符串参数，落盘为 JSON 字符串；
     * beforeQty/changeQty/afterQty 为 JSON 数字（本扫描用不到，不解析）。
     */
    @Data
    private static class TraceLogEntry {
        private String traceId;
        private String orderId;
        private Long userId;
        private Long voucherId;
        private String logType;
        /** 毫秒时间戳（JSON 字符串形态） */
        private String ts;
        /** 解析后的毫秒时间戳 */
        private long tsLong;
    }

    private void createTaskIfAbsent(SeckillReconcileAnomalyType anomalyType,
                                    Long orderId,
                                    Long userId,
                                    Long voucherId,
                                    String traceId,
                                    String detail) {
        // 幂等检查：先查再插 —— 对账 Job 每轮 scanOrders + scanDeductLogs 可能同时发现同一条异常，
        // 用业务维度（anomalyType + orderId/traceId）去重，避免同一笔异常生成多条 task。
        // 这里没用数据库唯一键的原因：orderId 和 traceId 组合不一定唯一（DEDUCT_PRESENT_ORDER_MISSING
        // 这种异常 orderId 本身就是 null，只能靠 traceId 定位），唯一键没法覆盖所有情况
        if (countActiveTask(anomalyType, orderId, traceId) > 0) {
            return;
        }
        // 补偿策略分级：
        // 只有 autoCompensationEnabled=true 且异常类型是 DEDUCT_PRESENT_ORDER_MISSING（DEDUCT 日志在但订单丢了）
        // 才走 pendingCompensation 路线 —— 这种异常最危险（库存真扣了但订单没了），补偿收益最高，
        // 但也因为最危险，才必须让开关严格控制，避免误补偿；
        // 其他所有情况（包括 ORDER_PRESENT_DEDUCT_MISSING、REDIS_DEDUCT_PRESENT_DB_MISSING）一律走
        // pending 等人工，保守策略
        SeckillReconcileTask task = autoCompensationEnabled
                && anomalyType == SeckillReconcileAnomalyType.DEDUCT_PRESENT_ORDER_MISSING
                ? SeckillReconcileTask.pendingCompensation(anomalyType, orderId, userId, voucherId, traceId, detail,
                compensationMaxRetryCount)
                : SeckillReconcileTask.pending(anomalyType, orderId, userId, voucherId, traceId, detail,
                compensationMaxRetryCount);
        taskMapper.insert(task);
        metricsRecorder.incReconcileAnomaly();
    }

    /**
     * 判断当前异常是否已经存在未处理的恢复任务 —— createTaskIfAbsent 的幂等检查支撑。
     *
     * <p>"active" 定义：排除 RESOLVED（已解决）和 IGNORED（已忽略）这两个终态，
     * 剩下的 PENDING / PENDING_COMPENSATION / PROCESSING / RETRYING 都算"活跃中"，
     * 意味着这条异常已经被系统接管了，不需要再建一条 task。</p>
     *
     * <p>定位维度的动态选择：
     * <ul>
     *   <li>orderId != null → 按 orderId 查（ORDER_PRESENT_DEDUCT_MISSING：订单在，orderId 有值）</li>
     *   <li>orderId == null → fallback 按 traceId 查（DEDUCT_PRESENT_ORDER_MISSING / REDIS_DEDUCT_PRESENT_DB_MISSING：
     *       DB 侧没有可定位的订单，orderId 本身就是 null，只能靠 traceId 这个贯穿全链路的标识来定位）</li>
     * </ul></p>
     */
    private long countActiveTask(SeckillReconcileAnomalyType anomalyType, Long orderId, String traceId) {
        QueryWrapper<SeckillReconcileTask> wrapper = new QueryWrapper<>();
        // anomalyType + "非终态"是固定条件 —— 同一类型的异常不管什么时候发现，只要还没闭环就视为活跃
        wrapper.eq("anomaly_type", anomalyType.name())
                .notIn("task_status", SeckillReconcileTaskStatus.RESOLVED.name(), SeckillReconcileTaskStatus.IGNORED.name());
        // 定位维度二选一：有 orderId 用 orderId，没有用 traceId（对应 createTaskIfAbsent 里两种异常形态的差异）
        if (orderId != null) {
            wrapper.eq("order_id", orderId);
        } else {
            wrapper.eq("trace_id", traceId);
        }
        // selectCount 返回 Number（MyBatis-Plus 泛型），null-safe 处理避免 NPE
        Number count = taskMapper.selectCount(wrapper);
        return count == null ? 0L : count.longValue();
    }
}
