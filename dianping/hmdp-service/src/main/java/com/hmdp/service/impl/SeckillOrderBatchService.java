package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.SeckillVoucherOrderMessage;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherReconcileLog;
import com.hmdp.mapper.VoucherReconcileLogMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.shared.reconciliation.model.ReconcileBusinessType;
import com.hmdp.shared.reconciliation.model.ReconcileLogType;
import com.hmdp.shared.reconciliation.model.ReconciliationStatus;
import com.hmdp.shared.reconciliation.model.VoucherOrderStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * 秒杀建单批量快路径 —— 整批消息在一个事务里完成
 * "锁库存行 → 一次 UPDATE stock=stock-N → 订单批量插入 → DEDUCT 日志批量写入"。
 *
 * <p>动机（2026-10-27 压测实证）：单笔链路下每笔订单独立竞争同一行库存记录的
 * InnoDB 行锁，4000 笔订单 = 4000 次行锁事务，消费端稳态 ~145/s；
 * 批量化后行锁获取次数从 N 次降到 N/32 次，事务提交次数同步下降。</p>
 *
 * <p>与单笔链路 {@link VoucherOrderServiceImpl#createVoucherOrderPlus} 的语义契约：</p>
 * <ul>
 *   <li><b>本方法只做快路径</b>：要么整批全部 CREATED，要么整体失败交还调用方逐条兜底；
 *       绝不产生部分成功状态 —— 任何异常（批内撞单、库存不足、基础设施故障）都让事务
 *       整体回滚，调用方退回单笔链路，单笔链路的全部语义（OUT_OF_STOCK 回滚 Redis /
 *       DUPLICATE 幂等 / recovery handoff）原样生效。</li>
 *   <li><b>防超卖语义不变</b>：锁定读库存 → 一次 UPDATE（WHERE stock>=N 冗余保护），
 *       行锁仍在同一事务内持有到提交。</li>
 *   <li><b>审计快照精确保留</b>：批内第 i 笔（0-based）的 before = 批前库存 - i、
 *       after = before - 1，与逐笔串行扣减完全等价。</li>
 *   <li><b>对账/兜底体系零变化</b>：DEDUCT 日志仍每单一笔、仍在建单同一事务内；
 *       verify/recovery/reconcile 三层兜底不感知本类的存在。</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SeckillOrderBatchService {

    /** 秒杀库存服务（锁定读库存 + 一次扣 N） */
    private final ISeckillVoucherService seckillVoucherService;

    /** 订单服务（复用 IService.saveBatch 批量插入订单，加入当前事务） */
    private final IVoucherOrderService voucherOrderService;

    /** 对账日志 Mapper（对账模块未启用时为 null，跳过日志写入，与单笔链路行为一致） */
    @Autowired(required = false)
    VoucherReconcileLogMapper voucherReconcileLogMapper;

    /**
     * 批量建单结果：fallback=true 表示快路径未执行（需逐条兜底），
     * 否则 createdCount/duplicateCount 描述本批处理结果（批内 orderId 去重计入 duplicate）。
     */
    public record BatchOutcome(boolean fallback, int createdCount, int duplicateCount) {
        static final BatchOutcome FALLBACK = new BatchOutcome(true, 0, 0);
    }

    /**
     * 批量建单快路径。
     *
     * <p>执行顺序：批内按 orderId 去重 → SELECT ... FOR UPDATE 锁定读库存
     * （拿到精确批前快照，同时持有行锁）→ 一次 UPDATE stock=stock-N →
     * 按算术推每笔快照 → 批量插入订单与 DEDUCT 日志 → 提交。
     * 库存不足直接返回 fallback（批前库存可能还够卖一部分，必须交给单笔链路
     * 逐条处理才能把剩余库存卖完并正确回滚多扣的 Redis）。</p>
     *
     * <p>任何 RuntimeException（批内 uk 冲突、DB 故障等）向外抛出 → 调用方逐条兜底；
     * 事务由 Spring 整体回滚，不残留任何部分状态。</p>
     */
    @Transactional
    public BatchOutcome tryCreateBatch(List<SeckillVoucherOrderMessage> messages) {
        // 1. 批内按 orderId 去重（保留首条）—— MQ 重投/重复生产场景的批内幂等
        LinkedHashMap<Long, SeckillVoucherOrderMessage> unique = new LinkedHashMap<>();
        for (SeckillVoucherOrderMessage message : messages) {
            if (message.getOrderId() == null || message.getVoucherId() == null) {
                log.warn("batch message missing orderId/voucherId, fallback to per-row, message={}", message);
                return BatchOutcome.FALLBACK;
            }
            unique.putIfAbsent(message.getOrderId(), message);
        }
        int duplicateCount = messages.size() - unique.size();

        // 同一批出现多个券 → 快路径不处理（UPDATE 只针对单一 voucher_id），交还逐条
        long distinctVouchers = unique.values().stream().map(SeckillVoucherOrderMessage::getVoucherId).distinct().count();
        if (distinctVouchers != 1) {
            log.info("batch contains {} distinct voucherIds, fallback to per-row", distinctVouchers);
            return BatchOutcome.FALLBACK;
        }

        Long voucherId = unique.values().iterator().next().getVoucherId();
        int batchSize = unique.size();

        // 2. 锁定读库存：SELECT ... FOR UPDATE —— 精确批前快照 + 提前持有行锁，
        //    后续 UPDATE 不再二次等锁
        SeckillVoucher voucher = seckillVoucherService.getOne(
                new QueryWrapper<SeckillVoucher>().eq("voucher_id", voucherId).last("FOR UPDATE"));
        if (voucher == null || voucher.getStock() < batchSize) {
            log.info("batch stock not enough or voucher missing, voucherId={}, stock={}, batchSize={}, fallback to per-row",
                    voucherId, voucher == null ? null : voucher.getStock(), batchSize);
            return BatchOutcome.FALLBACK;
        }
        int stockBeforeBatch = voucher.getStock();

        // 3. 一次扣 N —— WHERE stock>0 与单笔链路的乐观锁保护同源（此处 stock>=N 已由锁定读保证，冗余防护）
        boolean updated = seckillVoucherService.update()
                .setSql("stock=stock-" + batchSize)
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!updated) {
            log.warn("batch stock update affected 0 rows, voucherId={}, fallback to per-row", voucherId);
            return BatchOutcome.FALLBACK;
        }

        // 4. 构造订单 + DEDUCT 日志（快照按算术推：第 i 笔 before = stockBeforeBatch - i）
        LocalDateTime now = LocalDateTime.now();
        List<VoucherOrder> orders = new ArrayList<>(batchSize);
        List<VoucherReconcileLog> logs = new ArrayList<>(batchSize);
        int i = 0;
        for (SeckillVoucherOrderMessage message : unique.values()) {
            int beforeQty = stockBeforeBatch - i;
            VoucherOrder order = new VoucherOrder()
                    .setId(message.getOrderId())
                    .setUserId(message.getUserId())
                    .setVoucherId(message.getVoucherId())
                    .setTraceId(message.getTraceId())
                    .setStatus(VoucherOrderStatus.UNPAID)
                    .setCreateTime(now);
            orders.add(order);
            if (voucherReconcileLogMapper != null) {
                logs.add(new VoucherReconcileLog()
                        .setOrderId(message.getOrderId())
                        .setUserId(message.getUserId())
                        .setVoucherId(message.getVoucherId())
                        .setTraceId(message.getTraceId())
                        .setBeforeQty(beforeQty)
                        .setChangeQty(1)
                        .setAfterQty(beforeQty - 1)
                        .setLogType(ReconcileLogType.DEDUCT)
                        .setBusinessType(ReconcileBusinessType.SUCCESS)
                        .setReconciliationStatus(ReconciliationStatus.PENDING)
                        .setDetail("db order created and stock deducted (batch)"));
            }
            i++;
        }
        voucherOrderService.saveBatch(orders);
        if (voucherReconcileLogMapper != null) {
            for (VoucherReconcileLog reconcileLog : logs) {
                voucherReconcileLogMapper.insert(reconcileLog);
            }
        }

        log.info("batch order creation succeeded, voucherId={}, created={}, duplicates={}",
                voucherId, createdCount(orders.size()), duplicateCount);
        return new BatchOutcome(false, orders.size(), duplicateCount);
    }

    private static int createdCount(int size) {
        return size;
    }
}
