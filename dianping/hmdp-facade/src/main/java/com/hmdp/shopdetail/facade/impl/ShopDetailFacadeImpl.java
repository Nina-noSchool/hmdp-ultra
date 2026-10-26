package com.hmdp.shopdetail.facade.impl;

import com.hmdp.shopdetail.application.service.ShopDetailApplicationService;
import com.hmdp.shopdetail.domain.model.GuaranteeClaim;
import com.hmdp.shopdetail.domain.model.ShopDetailAggregate;
import com.hmdp.shopdetail.facade.ShopDetailFacade;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;

/**
 * 店铺详情门面实现
 * <p>
 * 纯委托实现，将所有调用转发给 {@link ShopDetailApplicationService}，不包含任何业务逻辑。
 * 它存在的意义是给外部一个稳定入口，方便后续替换应用层实现而不影响 controller / job / listener。
 * </p>
 */
@Service
public class ShopDetailFacadeImpl implements ShopDetailFacade {

    @Resource
    private ShopDetailApplicationService shopDetailApplicationService;

    /**
     * 查询店铺详情
     * <p>委托应用层：优先通过任务链引擎并行拉取商详聚合根，引擎未装配时降级为串行查询。</p>
     *
     * @param shopId 店铺主键
     * @return 商详聚合根（包含基础信息、评价、优惠券、博客、衍生标签等）
     */
    @Override
    public ShopDetailAggregate getShopDetail(Long shopId) {
        return shopDetailApplicationService.getShopDetail(shopId);
    }

    /**
     * 提交保障申诉单
     * <p>委托应用层：先持久化申诉单，再触发无忧闪赔状态机驱动异步补偿流。</p>
     *
     * @param claim 申诉单（调用方需已填充 shopId / userId / planCode / reason / claimNo）
     * @return 持久化后的申诉单（含创建时间与状态）
     */
    @Override
    public GuaranteeClaim submitGuaranteeClaim(GuaranteeClaim claim) {
        return shopDetailApplicationService.submitGuaranteeClaim(claim);
    }

    /**
     * 重试保障申诉
     * <p>委托应用层：按申诉单号查询后重新触发无忧闪赔状态机，超过最大重试次数时转人工审核。</p>
     *
     * @param claimNo 申诉单号（外部稳定回查用）
     * @return 重试后的申诉单（状态与结果消息已回写）
     */
    @Override
    public GuaranteeClaim retryGuaranteeClaim(String claimNo) {
        return shopDetailApplicationService.retryGuaranteeClaim(claimNo);
    }

    /**
     * 查询保障申诉最新状态
     * <p>委托领域层按申诉单号直查，不走状态机。</p>
     *
     * @param claimNo 申诉单号
     * @return 申诉单，不存在时返回 null
     */
    @Override
    public GuaranteeClaim getGuaranteeClaim(String claimNo) {
        return shopDetailApplicationService.getGuaranteeClaim(claimNo);
    }

    @Override
    public void warmupShopCache(Long shopId, Long expireSeconds) {
        shopDetailApplicationService.warmupShopCache(shopId, expireSeconds);
    }
}
