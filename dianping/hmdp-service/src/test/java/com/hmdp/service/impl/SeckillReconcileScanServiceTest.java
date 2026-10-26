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

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeckillReconcileScanServiceTest {

    @Test
    void scanRecentRecords_shouldCreateTaskWhenOrderExistsButDeductMissing() {
        SeckillReconcileTaskMapper taskMapper = mock(SeckillReconcileTaskMapper.class);
        VoucherOrderMapper orderMapper = mock(VoucherOrderMapper.class);
        VoucherReconcileLogMapper logMapper = mock(VoucherReconcileLogMapper.class);
        SeckillReconcileScanService service =
                new SeckillReconcileScanService(taskMapper, orderMapper, logMapper, new MetricsRecorder(new SimpleMeterRegistry()), 100, 3, 5, false);

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
        SeckillReconcileScanService service =
                new SeckillReconcileScanService(taskMapper, orderMapper, logMapper, new MetricsRecorder(new SimpleMeterRegistry()), 100, 3, 5, false);

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
        SeckillReconcileScanService service =
                new SeckillReconcileScanService(taskMapper, orderMapper, logMapper, new MetricsRecorder(new SimpleMeterRegistry()), 100, 3, 5, true);

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
}
