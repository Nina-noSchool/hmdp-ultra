package com.hmdp.shopdetail.job;

import com.hmdp.config.ShopBloomFilterConfig.ShopBloomFilterInitializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 店铺布隆过滤器周期重建任务（批次4）。
 *
 * <p>布隆只能 add 不能 remove，误判率随元素单调上升；店铺没有删除语义，
 * 日常无累积，但运营可能直接在 DB 里加店铺（不走 add 钩子）——每天凌晨全量
 * 重建一次，把误判率归零，同时覆盖任何旁路数据变更。</p>
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ShopBloomRebuildJob {

    private final ShopBloomFilterInitializer shopBloomFilterInitializer;

    @Scheduled(cron = "0 0 4 * * ?")
    public void rebuildShopBloom() {
        try {
            shopBloomFilterInitializer.rebuild("scheduled-daily");
        } catch (Exception e) {
            log.warn("shop bloom rebuild job failed, stay fail-open", e);
        }
    }
}
