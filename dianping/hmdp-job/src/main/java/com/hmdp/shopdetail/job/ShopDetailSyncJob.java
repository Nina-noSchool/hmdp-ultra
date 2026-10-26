package com.hmdp.shopdetail.job;

import com.hmdp.shopdetail.facade.ShopDetailFacade;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

/**
 * 店铺详情预热定时任务
 * <p>
 * 每 30 分钟触发一次，主动调用 getShopDetail 预热核心店铺的缓存，
 * 避免高峰期大量请求穿透到数据库。
 * 当前仅预热 shopId=1 的店铺，可按需扩展为批量预热。
 * </p>
 */
@Component
@Slf4j
public class ShopDetailSyncJob {

    @Resource
    private ShopDetailFacade shopDetailFacade;

    @Scheduled(cron = "0 0/30 * * * ?")
    public void warmupCoreShopDetail() {
        try {
            shopDetailFacade.getShopDetail(1L);
            shopDetailFacade.warmupShopCache(1L, 1800L);
        } catch (Exception e) {
            log.warn("shop detail warmup failed", e);
        }
    }
}
