package com.hmdp.shopdetail.infrastructure.repository;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.shopdetail.domain.model.GuaranteeClaim;
import com.hmdp.shopdetail.infrastructure.persistence.mapper.GuaranteeClaimMapper;
import org.springframework.stereotype.Repository;

import jakarta.annotation.Resource;
import java.util.Optional;

/**
 * 保障申诉仓储实现（基础设施层）
 * <p>
 * 通过 MyBatis Plus 操作 tb_shop_guarantee_claim 表。
 * save 方法根据 id 是否为 null 自动选择 insert 或 update。
 * </p>
 */
@Repository
public class GuaranteeClaimRepository implements com.hmdp.shopdetail.domain.repository.GuaranteeClaimRepository {

    @Resource
    private GuaranteeClaimMapper guaranteeClaimMapper;

    @Override
    public GuaranteeClaim save(GuaranteeClaim claim) {
        com.hmdp.shopdetail.infrastructure.persistence.dataobject.GuaranteeClaim dataObject = toDataObject(claim);
        if (dataObject.getId() == null) {
            guaranteeClaimMapper.insert(dataObject);
        } else {
            guaranteeClaimMapper.updateById(dataObject);
        }
        claim.setId(dataObject.getId());
        return claim;
    }

    @Override
    public Optional<GuaranteeClaim> findByClaimNo(String claimNo) {
        QueryWrapper<com.hmdp.shopdetail.infrastructure.persistence.dataobject.GuaranteeClaim> wrapper = new QueryWrapper<>();
        wrapper.eq("claim_no", claimNo).last("limit 1");
        com.hmdp.shopdetail.infrastructure.persistence.dataobject.GuaranteeClaim dataObject = guaranteeClaimMapper.selectOne(wrapper);
        return Optional.ofNullable(dataObject).map(this::toDomain);
    }

    private com.hmdp.shopdetail.infrastructure.persistence.dataobject.GuaranteeClaim toDataObject(GuaranteeClaim claim) {
        com.hmdp.shopdetail.infrastructure.persistence.dataobject.GuaranteeClaim dataObject = new com.hmdp.shopdetail.infrastructure.persistence.dataobject.GuaranteeClaim();
        dataObject.setId(claim.getId());
        dataObject.setShopId(claim.getShopId());
        dataObject.setUserId(claim.getUserId());
        dataObject.setClaimNo(claim.getClaimNo());
        dataObject.setPlanCode(claim.getPlanCode());
        dataObject.setReason(claim.getReason());
        dataObject.setStatus(claim.getStatus());
        dataObject.setResultMessage(claim.getResultMessage());
        dataObject.setCreateTime(claim.getCreateTime());
        return dataObject;
    }

    private GuaranteeClaim toDomain(com.hmdp.shopdetail.infrastructure.persistence.dataobject.GuaranteeClaim dataObject) {
        GuaranteeClaim claim = new GuaranteeClaim();
        claim.setId(dataObject.getId());
        claim.setShopId(dataObject.getShopId());
        claim.setUserId(dataObject.getUserId());
        claim.setClaimNo(dataObject.getClaimNo());
        claim.setPlanCode(dataObject.getPlanCode());
        claim.setReason(dataObject.getReason());
        claim.setStatus(dataObject.getStatus());
        claim.setResultMessage(dataObject.getResultMessage());
        claim.setCreateTime(dataObject.getCreateTime());
        return claim;
    }
}
