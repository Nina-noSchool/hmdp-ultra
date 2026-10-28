package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.hmdp.dto.SeckillVoucherOrderMessage;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherReconcileLog;
import com.hmdp.mapper.VoucherReconcileLogMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link SeckillOrderBatchService} 单元测试 —— 批量快路径的语义契约验证。
 *
 * <p>覆盖四条核心路径：
 * ① 库存充足 → 整批 CREATED + 快照算术精确 + 日志每单一笔；
 * ② 库存不足 → fallback=true，不写任何数据（留给单笔链路把剩余库存卖完）；
 * ③ 批内 orderId 去重 → 重复消息计 duplicate，只按去重后数量扣库存；
 * ④ 任何异常向外抛 → 调用方逐条兜底（事务回滚由 Spring 负责）。</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SeckillOrderBatchServiceTest {

    @Mock
    private ISeckillVoucherService seckillVoucherService;

    @Mock
    private IVoucherOrderService voucherOrderService;

    @Mock
    private VoucherReconcileLogMapper voucherReconcileLogMapper;

    private SeckillOrderBatchService batchService;

    @BeforeEach
    void setUp() {
        batchService = new SeckillOrderBatchService(seckillVoucherService, voucherOrderService);
        batchService.voucherReconcileLogMapper = voucherReconcileLogMapper;
        when(voucherOrderService.saveBatch(anyList())).thenReturn(true);
        when(voucherReconcileLogMapper.insert(any(VoucherReconcileLog.class))).thenReturn(1);
    }

    private SeckillVoucherOrderMessage message(long orderId, long userId) {
        SeckillVoucherOrderMessage message = new SeckillVoucherOrderMessage();
        message.setOrderId(orderId);
        message.setUserId(userId);
        message.setVoucherId(13L);
        message.setTraceId("trace-" + orderId);
        return message;
    }

    private void stubStock(int stock) {
        SeckillVoucher voucher = new SeckillVoucher();
        voucher.setVoucherId(13L);
        voucher.setStock(stock);
        when(seckillVoucherService.getOne(any(Wrapper.class))).thenReturn(voucher);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void stubUpdateSuccess() {
        com.baomidou.mybatisplus.extension.conditions.update.UpdateChainWrapper chain =
                org.mockito.Mockito.mock(com.baomidou.mybatisplus.extension.conditions.update.UpdateChainWrapper.class);
        when(seckillVoucherService.update()).thenReturn(chain);
        when(chain.setSql(anyString())).thenReturn(chain);
        when(chain.eq(anyString(), any())).thenReturn(chain);
        when(chain.gt(anyString(), any())).thenReturn(chain);
        when(chain.update()).thenReturn(true);
    }

    @Test
    void successBatchWritesOrdersAndExactSnapshots() {
        stubStock(100);
        stubUpdateSuccess();

        SeckillOrderBatchService.BatchOutcome outcome = batchService.tryCreateBatch(List.of(
                message(1L, 101L),
                message(2L, 102L),
                message(3L, 103L)
        ));

        assertFalse(outcome.fallback());
        assertEquals(3, outcome.createdCount());
        assertEquals(0, outcome.duplicateCount());

        // 一次扣 N=3
        verify(seckillVoucherService.update()).setSql("stock=stock-3");

        // 订单批量插入：3 笔，字段来自消息
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<VoucherOrder>> orderCaptor = ArgumentCaptor.forClass((Class) List.class);
        verify(voucherOrderService).saveBatch(orderCaptor.capture());
        List<VoucherOrder> orders = orderCaptor.getValue();
        assertEquals(3, orders.size());
        assertEquals(1L, orders.get(0).getId());
        assertEquals(101L, orders.get(0).getUserId());

        // DEDUCT 日志每单一笔，快照与逐笔串行扣减完全等价（before=100-i, after=before-1）
        ArgumentCaptor<VoucherReconcileLog> logCaptor = ArgumentCaptor.forClass(VoucherReconcileLog.class);
        verify(voucherReconcileLogMapper, times(3)).insert(logCaptor.capture());
        List<VoucherReconcileLog> logs = logCaptor.getAllValues();
        assertEquals(100, logs.get(0).getBeforeQty());
        assertEquals(99, logs.get(0).getAfterQty());
        assertEquals(99, logs.get(1).getBeforeQty());
        assertEquals(98, logs.get(1).getAfterQty());
        assertEquals(98, logs.get(2).getBeforeQty());
        assertEquals(97, logs.get(2).getAfterQty());
    }

    @Test
    void insufficientStockFallsBackWithoutWrites() {
        stubStock(2);

        SeckillOrderBatchService.BatchOutcome outcome = batchService.tryCreateBatch(List.of(
                message(1L, 101L),
                message(2L, 102L),
                message(3L, 103L)
        ));

        // 库存不足不硬拒：交还单笔链路把剩余库存卖完并正确回滚多扣的 Redis
        assertTrue(outcome.fallback());
        verify(seckillVoucherService, never()).update();
        verify(voucherOrderService, never()).saveBatch(anyList());
        verify(voucherReconcileLogMapper, never()).insert(any(VoucherReconcileLog.class));
    }

    @Test
    void inBatchDuplicateOrderIdsAreCountedAndSkipped() {
        stubStock(50);
        stubUpdateSuccess();

        SeckillOrderBatchService.BatchOutcome outcome = batchService.tryCreateBatch(List.of(
                message(1L, 101L),
                message(1L, 101L),
                message(2L, 102L)
        ));

        assertFalse(outcome.fallback());
        assertEquals(2, outcome.createdCount());
        assertEquals(1, outcome.duplicateCount());
        verify(seckillVoucherService.update()).setSql("stock=stock-2");
        verify(voucherOrderService).saveBatch(anyList());
        verify(voucherReconcileLogMapper, times(2)).insert(any(VoucherReconcileLog.class));
    }

    @Test
    void missingVoucherFallsBack() {
        when(seckillVoucherService.getOne(any(Wrapper.class))).thenReturn(null);

        SeckillOrderBatchService.BatchOutcome outcome =
                batchService.tryCreateBatch(List.of(message(1L, 101L)));

        assertTrue(outcome.fallback());
        verify(voucherOrderService, never()).saveBatch(anyList());
    }

    @Test
    void exceptionPropagatesForPerRowFallback() {
        stubStock(100);
        stubUpdateSuccess();
        when(voucherOrderService.saveBatch(anyList())).thenThrow(new RuntimeException("db down"));

        assertThrows(RuntimeException.class,
                () -> batchService.tryCreateBatch(List.of(message(1L, 101L))));
    }
}
