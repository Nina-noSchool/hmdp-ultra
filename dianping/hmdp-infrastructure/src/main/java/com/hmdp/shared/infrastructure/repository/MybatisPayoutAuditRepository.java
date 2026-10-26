package com.hmdp.shared.infrastructure.repository;

import com.hmdp.shared.domain.compensation.spi.PayoutAuditRecord;
import com.hmdp.shared.domain.compensation.spi.PayoutAuditRepository;
import com.hmdp.shared.infrastructure.persistence.entity.PayoutAuditLogEntity;
import com.hmdp.shared.infrastructure.persistence.mapper.PayoutAuditLogMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;

import jakarta.annotation.Resource;

/**
 * 赔付审计仓储 MyBatis 实现。
 *
 * <p>唯一键 claim_no 保证幂等：重复 save 时捕获 DuplicateKeyException 静默忽略。</p>
 */
@Slf4j
@Repository
public class MybatisPayoutAuditRepository implements PayoutAuditRepository {

    @Resource
    private PayoutAuditLogMapper payoutAuditLogMapper;

    @Override
    public void save(PayoutAuditRecord record) {
        PayoutAuditLogEntity entity = new PayoutAuditLogEntity();
        entity.setClaimNo(record.getClaimNo());
        entity.setShopId(record.getShopId());
        entity.setUserId(record.getUserId());
        entity.setPlanCode(record.getPlanCode());
        entity.setPayoutAmount(record.getPayoutAmount());
        entity.setPaymentTxId(record.getPaymentTxId());
        entity.setStatus(record.getStatus());
        entity.setMessage(record.getMessage());
        entity.setCreateTime(record.getCreateTime());
        try {
            payoutAuditLogMapper.insert(entity);
        } catch (DuplicateKeyException e) {
            log.info("[PayoutAudit] duplicate claimNo={}, skip", record.getClaimNo());
        }
    }
}