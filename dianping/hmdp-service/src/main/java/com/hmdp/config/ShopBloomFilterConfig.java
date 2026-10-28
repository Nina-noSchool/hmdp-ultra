package com.hmdp.config;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisBloomFilter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;

/**
 * 店铺布隆过滤器装配（批次4）。
 *
 * <p>防穿透防线的演进关系（面试叙事）：
 * 负缓存（空值兜底，已上线）→ 布隆前置守卫（本组件：确定不存在的 id 在进缓存/DB 前就拒绝）。
 * 两者是叠加关系：布隆误判"存在"的请求落到负缓存兜底，布隆绝不误杀真实数据（fail-open 设计）。</p>
 *
 * <p>开关：{@code bloom.shop.enabled}（默认 true）。关闭时两个 Bean 都不装配，
 * ShopServiceImpl 的守卫字段为 null，行为与没有这层完全一致。</p>
 */
@Configuration
public class ShopBloomFilterConfig {

    @Bean
    @ConditionalOnProperty(name = "bloom.shop.enabled", havingValue = "true", matchIfMissing = true)
    public RedisBloomFilter shopBloomFilter(StringRedisTemplate template,
                                            @Value("${bloom.shop.key:bloom:shop}") String key,
                                            @Value("${bloom.shop.expected-insertions:100000}") long expectedInsertions,
                                            @Value("${bloom.shop.fpp:0.01}") double fpp) {
        return new RedisBloomFilter(template, key, expectedInsertions, fpp);
    }

    @Bean
    @ConditionalOnProperty(name = "bloom.shop.enabled", havingValue = "true", matchIfMissing = true)
    public ShopBloomFilterInitializer shopBloomFilterInitializer(RedisBloomFilter shopBloomFilter,
                                                                 IShopService shopService) {
        return new ShopBloomFilterInitializer(shopBloomFilter, shopService);
    }

    /**
     * 启动时全量重建（店铺表量级小，同步执行）；也供定时 Job 调用做周期重建
     * （覆盖"运营直接在 DB 里加店铺"这类没有走 add 钩子的数据变更）。
     */
    @Slf4j
    @RequiredArgsConstructor
    public static class ShopBloomFilterInitializer implements ApplicationRunner {

        private final RedisBloomFilter filter;
        private final IShopService shopService;

        @Override
        public void run(ApplicationArguments args) {
            rebuild("startup");
        }

        /** 全量重同步；失败时布隆停留在 fail-open 状态（放行），等下一次同步恢复，绝不误杀 */
        public void rebuild(String source) {
            try {
                List<Long> ids = shopService.lambdaQuery().select(Shop::getId).list()
                        .stream().map(Shop::getId).toList();
                filter.resync(RedisBloomFilter.toLongArray(ids));
            } catch (Exception e) {
                log.warn("shop bloom filter resync failed, stay fail-open, source={}", source, e);
            }
        }
    }
}
