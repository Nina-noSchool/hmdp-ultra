package com.hmdp.service.impl;

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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeckillReconcileScanServiceTest {

    private static final String TRACE_KEY = "seckill:trace:log:21";
    private static final String GHOST_TRACE_ID = "trace-999";

    @Test
    void scanRecentRecords_shouldCreateTaskWhenOrderExistsButDeductMissing() {
        SeckillReconcileTaskMapper taskMapper = mock(SeckillReconcileTaskMapper.class);
        VoucherOrderMapper orderMapper = mock(VoucherOrderMapper.class);
        VoucherReconcileLogMapper logMapper = mock(VoucherReconcileLogMapper.class);
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        SeckillReconcileScanService service = new SeckillReconcileScanService(
                taskMapper, orderMapper, logMapper, new MetricsRecorder(new SimpleMeterRegistry()), redisTemplate,
                100, 3, 5, false, false);

        VoucherOrder order = new VoucherOrder()
                .setId(101L)
                .setUserId(11L)
                .setVoucherId(21L)
                .setCreateTime(LocalDateTime.now().minusMinutes(5));
        when(orderMapper.selectList(any())).thenReturn(Collections.singletonList(order));
        when(logMapper.selectOne(any())).thenReturn(null);
        when(logMapper.selectList(any())).thenReturn(Collections.emptyList());
        when(taskMapper.selectCount(any())).thenReturn(0L);

        service.scanRecentRecords();

        verify(taskMapper).insert(any(SeckillReconcileTask.class));
    }

    @Test
    void scanRecentRecords_shouldNotCreateDuplicateActiveTask() {
        SeckillReconcileTaskMapper taskMapper = mock(SeckillReconcileTaskMapper.class);
        VoucherOrderMapper orderMapper = mock(VoucherOrderMapper.class);
        VoucherReconcileLogMapper logMapper = mock(VoucherReconcileLogMapper.class);
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        SeckillReconcileScanService service = new SeckillReconcileScanService(
                taskMapper, orderMapper, logMapper, new MetricsRecorder(new SimpleMeterRegistry()), redisTemplate,
                100, 3, 5, false, false);

        VoucherOrder order = new VoucherOrder()
                .setId(101L)
                .setUserId(11L)
                .setVoucherId(21L)
                .setCreateTime(LocalDateTime.now().minusMinutes(5));
        when(orderMapper.selectList(any())).thenReturn(Collections.singletonList(order));
        when(logMapper.selectOne(any())).thenReturn(null);
        when(logMapper.selectList(any())).thenReturn(Collections.emptyList());
        when(taskMapper.selectCount(any())).thenReturn(1L);

        service.scanRecentRecords();

        verify(taskMapper, never()).insert(any(SeckillReconcileTask.class));
    }

    @Test
    void scanRecentRecords_shouldCreateWaitingCompensationTaskForDeductWithoutOrderWhenAutoCompensationEnabled() {
        SeckillReconcileTaskMapper taskMapper = mock(SeckillReconcileTaskMapper.class);
        VoucherOrderMapper orderMapper = mock(VoucherOrderMapper.class);
        VoucherReconcileLogMapper logMapper = mock(VoucherReconcileLogMapper.class);
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        SeckillReconcileScanService service = new SeckillReconcileScanService(
                taskMapper, orderMapper, logMapper, new MetricsRecorder(new SimpleMeterRegistry()), redisTemplate,
                100, 3, 5, true, false);

        VoucherReconcileLog deductLog = new VoucherReconcileLog()
                .setOrderId(101L)
                .setUserId(11L)
                .setVoucherId(21L)
                .setTraceId("trace-101")
                .setLogType(ReconcileLogType.DEDUCT)
                .setCreateTime(LocalDateTime.now().minusMinutes(5));
        when(orderMapper.selectList(any())).thenReturn(Collections.emptyList());
        when(logMapper.selectList(any())).thenReturn(Collections.singletonList(deductLog));
        when(orderMapper.selectById(101L)).thenReturn(null);
        when(logMapper.selectOne(any())).thenReturn(null);
        when(taskMapper.selectCount(any())).thenReturn(0L);

        service.scanRecentRecords();

        verify(taskMapper).insert(any(SeckillReconcileTask.class));
    }

    // ==================== 第三扫描方向：Redis 追踪日志（trace-scan-enabled） ====================

    @Test
    void traceScanDisabled_shouldNotTouchRedisAtAll() {
        SeckillReconcileTaskMapper taskMapper = mock(SeckillReconcileTaskMapper.class);
        VoucherOrderMapper orderMapper = mock(VoucherOrderMapper.class);
        VoucherReconcileLogMapper logMapper = mock(VoucherReconcileLogMapper.class);
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        SeckillReconcileScanService service = new SeckillReconcileScanService(
                taskMapper, orderMapper, logMapper, new MetricsRecorder(new SimpleMeterRegistry()), redisTemplate,
                100, 3, 5, false, false);
        when(orderMapper.selectList(any())).thenReturn(Collections.emptyList());
        when(logMapper.selectList(any())).thenReturn(Collections.emptyList());

        service.scanRecentRecords();

        // 开关默认关 → scanRecentRecords 行为与改动前一致，Redis 侧零交互（回归用例）
        verify(redisTemplate, never()).scan(any(ScanOptions.class));
        verify(redisTemplate, never()).opsForHash();
        verify(taskMapper, never()).insert(any(SeckillReconcileTask.class));
    }

    @Test
    void traceScan_shouldSkipInFlightEntryInsideDelayWindow() {
        SeckillReconcileTaskMapper taskMapper = mock(SeckillReconcileTaskMapper.class);
        VoucherOrderMapper orderMapper = mock(VoucherOrderMapper.class);
        VoucherReconcileLogMapper logMapper = mock(VoucherReconcileLogMapper.class);
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        SeckillReconcileScanService service = new SeckillReconcileScanService(
                taskMapper, orderMapper, logMapper, new MetricsRecorder(new SimpleMeterRegistry()), redisTemplate,
                100, 3, 5, false, true);

        String freshTs = String.valueOf(System.currentTimeMillis());
        stubTraceScan(redisTemplate, mockHashOps(),
                Map.<Object, Object>entry(GHOST_TRACE_ID, deductEntry(GHOST_TRACE_ID, "999", freshTs)));
        when(orderMapper.selectList(any())).thenReturn(Collections.emptyList());
        when(logMapper.selectList(any())).thenReturn(Collections.emptyList());

        service.scanRecentRecords();

        // 在途窗口内（3 分钟内扣的）→ 不查 DB、不建工单
        verify(logMapper, never()).selectOne(any());
        verify(taskMapper, never()).insert(any(SeckillReconcileTask.class));
    }

    @Test
    void traceScan_shouldSkipRestoreEntry() {
        SeckillReconcileTaskMapper taskMapper = mock(SeckillReconcileTaskMapper.class);
        VoucherOrderMapper orderMapper = mock(VoucherOrderMapper.class);
        VoucherReconcileLogMapper logMapper = mock(VoucherReconcileLogMapper.class);
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        SeckillReconcileScanService service = new SeckillReconcileScanService(
                taskMapper, orderMapper, logMapper, new MetricsRecorder(new SimpleMeterRegistry()), redisTemplate,
                100, 3, 5, false, true);

        String oldTs = String.valueOf(System.currentTimeMillis() - 10 * 60_000L);
        String restoreEntry = "{\"traceId\":\"" + GHOST_TRACE_ID + "\",\"orderId\":\"999\",\"userId\":\"11\","
                + "\"voucherId\":\"21\",\"beforeQty\":5,\"changeQty\":1,\"afterQty\":4,"
                + "\"logType\":\"RESTORE\",\"ts\":\"" + oldTs + "\"}";
        stubTraceScan(redisTemplate, mockHashOps(),
                Map.<Object, Object>entry(GHOST_TRACE_ID, restoreEntry));
        when(orderMapper.selectList(any())).thenReturn(Collections.emptyList());
        when(logMapper.selectList(any())).thenReturn(Collections.emptyList());

        service.scanRecentRecords();

        // 回滚脚本覆盖同一 field 写成 RESTORE → 直接跳过，不查 DB、不建工单
        verify(logMapper, never()).selectOne(any());
        verify(taskMapper, never()).insert(any(SeckillReconcileTask.class));
    }

    @Test
    void traceScan_shouldSkipWhenDbAlreadyHasRestoreLog() {
        SeckillReconcileTaskMapper taskMapper = mock(SeckillReconcileTaskMapper.class);
        VoucherOrderMapper orderMapper = mock(VoucherOrderMapper.class);
        VoucherReconcileLogMapper logMapper = mock(VoucherReconcileLogMapper.class);
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        SeckillReconcileScanService service = new SeckillReconcileScanService(
                taskMapper, orderMapper, logMapper, new MetricsRecorder(new SimpleMeterRegistry()), redisTemplate,
                100, 3, 5, false, true);

        String oldTs = String.valueOf(System.currentTimeMillis() - 10 * 60_000L);
        stubTraceScan(redisTemplate, mockHashOps(),
                Map.<Object, Object>entry(GHOST_TRACE_ID, deductEntry(GHOST_TRACE_ID, "999", oldTs)));
        when(orderMapper.selectList(any())).thenReturn(Collections.emptyList());
        when(logMapper.selectList(any())).thenReturn(Collections.emptyList());
        // Redis 侧还是 DEDUCT，但 DB 已经落了 RESTORE 对账日志 → 已回滚，跳过
        when(logMapper.selectOne(any())).thenReturn(new VoucherReconcileLog()
                .setTraceId(GHOST_TRACE_ID)
                .setLogType(ReconcileLogType.RESTORE));

        service.scanRecentRecords();

        verify(taskMapper, never()).insert(any(SeckillReconcileTask.class));
    }

    @Test
    void traceScan_shouldSkipWhenOrderExistsInDb() {
        SeckillReconcileTaskMapper taskMapper = mock(SeckillReconcileTaskMapper.class);
        VoucherOrderMapper orderMapper = mock(VoucherOrderMapper.class);
        VoucherReconcileLogMapper logMapper = mock(VoucherReconcileLogMapper.class);
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        SeckillReconcileScanService service = new SeckillReconcileScanService(
                taskMapper, orderMapper, logMapper, new MetricsRecorder(new SimpleMeterRegistry()), redisTemplate,
                100, 3, 5, false, true);

        String oldTs = String.valueOf(System.currentTimeMillis() - 10 * 60_000L);
        stubTraceScan(redisTemplate, mockHashOps(),
                Map.<Object, Object>entry(GHOST_TRACE_ID, deductEntry(GHOST_TRACE_ID, "999", oldTs)));
        when(orderMapper.selectList(any())).thenReturn(Collections.emptyList());
        when(logMapper.selectList(any())).thenReturn(Collections.emptyList());
        when(logMapper.selectOne(any())).thenReturn(null);
        when(orderMapper.selectById(999L)).thenReturn(new VoucherOrder().setId(999L));

        service.scanRecentRecords();

        verify(taskMapper, never()).insert(any(SeckillReconcileTask.class));
    }

    @Test
    void traceScan_shouldCreatePendingTaskWhenDbHasNoTraceAtAll() {
        SeckillReconcileTaskMapper taskMapper = mock(SeckillReconcileTaskMapper.class);
        VoucherOrderMapper orderMapper = mock(VoucherOrderMapper.class);
        VoucherReconcileLogMapper logMapper = mock(VoucherReconcileLogMapper.class);
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        SeckillReconcileScanService service = new SeckillReconcileScanService(
                taskMapper, orderMapper, logMapper, new MetricsRecorder(new SimpleMeterRegistry()), redisTemplate,
                100, 3, 5, false, true);

        String oldTs = String.valueOf(System.currentTimeMillis() - 10 * 60_000L);
        stubTraceScan(redisTemplate, mockHashOps(),
                Map.<Object, Object>entry(GHOST_TRACE_ID, deductEntry(GHOST_TRACE_ID, "999", oldTs)));
        when(orderMapper.selectList(any())).thenReturn(Collections.emptyList());
        when(logMapper.selectList(any())).thenReturn(Collections.emptyList());
        when(logMapper.selectOne(any())).thenReturn(null);
        when(orderMapper.selectById(999L)).thenReturn(null);
        // 第一次 selectCount：按 traceId 查 DB 任何对账日志（无）；第二次：查重活跃工单（无）
        when(logMapper.selectCount(any())).thenReturn(0L);
        when(taskMapper.selectCount(any())).thenReturn(0L);

        service.scanRecentRecords();

        ArgumentCaptor<SeckillReconcileTask> captor = ArgumentCaptor.forClass(SeckillReconcileTask.class);
        verify(taskMapper).insert(captor.capture());
        SeckillReconcileTask task = captor.getValue();
        assertEquals(SeckillReconcileAnomalyType.REDIS_DEDUCT_PRESENT_DB_MISSING.name(), task.getAnomalyType());
        // 一律 pending 转人工（补偿状态 NOT_NEEDED），不给这个异常类型开自动补偿
        assertEquals(SeckillReconcileTaskStatus.PENDING.name(), task.getTaskStatus());
        assertEquals(SeckillReconcileCompensationStatus.NOT_NEEDED.name(), task.getCompensationStatus());
        // DB 没订单 → orderId 不作为定位键（null），查重与定位走 traceId 分支
        assertNull(task.getOrderId());
        assertEquals(GHOST_TRACE_ID, task.getTraceId());
        assertEquals(21L, task.getVoucherId());
        assertTrue(task.getAnomalyDetail().contains("orderId=999"));
        // 证据随工单走：完整追踪快照进 detail（追踪日志 TTL 2 天，人工介入时 Redis 侧可能已过期）
        assertTrue(task.getAnomalyDetail().contains("\"traceId\":\"" + GHOST_TRACE_ID + "\""));
        assertTrue(task.getAnomalyDetail().contains("\"beforeQty\":5"));
        assertTrue(task.getAnomalyDetail().contains("\"afterQty\":4"));
        // reconcileNo 兜底用 traceId（orderId 为 null）
        assertTrue(task.getReconcileNo().contains(GHOST_TRACE_ID));
    }

    @Test
    void traceScan_repeatScan_shouldNotDuplicateActiveTask() {
        SeckillReconcileTaskMapper taskMapper = mock(SeckillReconcileTaskMapper.class);
        VoucherOrderMapper orderMapper = mock(VoucherOrderMapper.class);
        VoucherReconcileLogMapper logMapper = mock(VoucherReconcileLogMapper.class);
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        SeckillReconcileScanService service = new SeckillReconcileScanService(
                taskMapper, orderMapper, logMapper, new MetricsRecorder(new SimpleMeterRegistry()), redisTemplate,
                100, 3, 5, false, true);

        String oldTs = String.valueOf(System.currentTimeMillis() - 10 * 60_000L);
        stubTraceScan(redisTemplate, mockHashOps(),
                Map.<Object, Object>entry(GHOST_TRACE_ID, deductEntry(GHOST_TRACE_ID, "999", oldTs)));
        when(orderMapper.selectList(any())).thenReturn(Collections.emptyList());
        when(logMapper.selectList(any())).thenReturn(Collections.emptyList());
        when(logMapper.selectOne(any())).thenReturn(null);
        when(orderMapper.selectById(999L)).thenReturn(null);
        // 第一轮：DB 无任何对账日志（0）→ 走到查重；已有活跃工单（1）→ 不重复建单。
        // selectCount 固定调用顺序：先"DB 对账日志存在性"，后"活跃工单查重"
        when(logMapper.selectCount(any())).thenReturn(0L);
        when(taskMapper.selectCount(any())).thenReturn(1L);

        service.scanRecentRecords();

        verify(taskMapper, never()).insert(any(SeckillReconcileTask.class));
    }

    // ==================== 打桩辅助 ====================

    @SuppressWarnings("unchecked")
    private HashOperations<String, Object, Object> mockHashOps() {
        return mock(HashOperations.class);
    }

    private void stubTraceScan(StringRedisTemplate redisTemplate,
                               HashOperations<String, Object, Object> hashOps,
                               Map.Entry<Object, Object>... entries) {
        // 游标必须在 when(...) 之前构造：thenReturn 的参数求值发生在打桩完成前，
        // cursorOf 内部的 when(...) 会被 Mockito 判定为嵌套打桩（UnfinishedStubbing）
        Cursor<String> keysCursor = cursorOf(TRACE_KEY);
        Cursor<Map.Entry<Object, Object>> entryCursor = cursorOf(entries);
        when(redisTemplate.scan(any(ScanOptions.class))).thenReturn(keysCursor);
        when(redisTemplate.opsForHash()).thenReturn(hashOps);
        when(hashOps.scan(eq(TRACE_KEY), any(ScanOptions.class))).thenReturn(entryCursor);
    }

    /** 构造追踪日志 field 的 JSON（与 seckill_plus.lua cjson.encode 结构一致） */
    private static String deductEntry(String traceId, String orderId, String ts) {
        return "{\"traceId\":\"" + traceId + "\",\"orderId\":\"" + orderId + "\",\"userId\":\"11\","
                + "\"voucherId\":\"21\",\"beforeQty\":5,\"changeQty\":1,\"afterQty\":4,"
                + "\"logType\":\"DEDUCT\",\"ts\":\"" + ts + "\"}";
    }

    /** 用队列模拟 Redis 游标：hasNext = 队列非空，next = 出队 */
    @SuppressWarnings("unchecked")
    private <T> Cursor<T> cursorOf(T... items) {
        Queue<T> queue = new LinkedList<>(Arrays.asList(items));
        Cursor<T> cursor = mock(Cursor.class);
        when(cursor.hasNext()).thenAnswer(inv -> !queue.isEmpty());
        when(cursor.next()).thenAnswer(inv -> queue.poll());
        return cursor;
    }
}
