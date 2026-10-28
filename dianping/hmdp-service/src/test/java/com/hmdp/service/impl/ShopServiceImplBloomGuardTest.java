package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.MultiLevelCacheService;
import com.hmdp.utils.RedisBloomFilter;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 店铺查询的布隆前置守卫接线测试。
 *
 * <p>验证三件事：
 * ① 布隆判定"确定不存在" → 直接拒绝，多级缓存/负缓存/DB 一整条链都不进；
 * ② 布隆放行（可能存在 / fail-open）→ 正常走多级缓存链路，行为与无守卫时一致；
 * ③ 守卫 Bean 未装配（开关关闭）→ 字段为 null，完全等价于无守卫。</p>
 */
class ShopServiceImplBloomGuardTest {

    private static final String FAIL_MSG = "店铺不存在！";

    private ShopServiceImpl shopService;
    private RedisBloomFilter bloomFilter;
    private MultiLevelCacheService multiLevelCacheService;
    private Shop shop;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        shopService = new ShopServiceImpl();
        bloomFilter = mock(RedisBloomFilter.class);
        multiLevelCacheService = mock(MultiLevelCacheService.class);
        shop = new Shop().setId(13L).setName("测试奶茶店");

        ReflectionTestUtils.setField(shopService, "shopBloomFilter", bloomFilter);
        ReflectionTestUtils.setField(shopService, "multiLevelCacheService", multiLevelCacheService);
        ReflectionTestUtils.setField(shopService, "localCacheEnabled", true);
        ReflectionTestUtils.setField(shopService, "clientClient", mock(CacheClient.class));

        when(multiLevelCacheService.queryWithLocalCache(
                anyString(), anyLong(), eq(Shop.class), any(Function.class),
                any(), any(TimeUnit.class))).thenReturn(shop);
    }

    @Test
    void shouldRejectWithoutTouchingCacheChainWhenBloomSaysAbsent() {
        when(bloomFilter.mightContain(999L)).thenReturn(false);

        Result result = shopService.queryById(999L);

        assertFalse(result.getSuccess());
        assertEquals(FAIL_MSG, result.getErrorMsg());
        assertNull(result.getData());
        // 关键断言：缓存链一整条都没进——穿透在事前被拦下
        verify(multiLevelCacheService, never())
                .queryWithLocalCache(anyString(), anyLong(), eq(Shop.class), any(Function.class),
                        any(), any(TimeUnit.class));
    }

    @Test
    void shouldPassThroughToMultiLevelCacheWhenBloomSaysPossible() {
        when(bloomFilter.mightContain(13L)).thenReturn(true);

        Result result = shopService.queryById(13L);

        assertTrue(result.getSuccess());
        assertEquals(shop, result.getData());
        ArgumentCaptor<Long> idCaptor = ArgumentCaptor.forClass(Long.class);
        verify(multiLevelCacheService).queryWithLocalCache(
                eq(RedisConstants.CACHE_SHOP_KEY), idCaptor.capture(), eq(Shop.class),
                any(Function.class), any(), any(TimeUnit.class));
        assertEquals(13L, idCaptor.getValue());
    }

    @Test
    void shouldBehaveAsUnguardedWhenBeanAbsent() {
        // bloom.shop.enabled=false：Bean 不装配，字段为 null，行为与无守卫完全一致
        ReflectionTestUtils.setField(shopService, "shopBloomFilter", null);

        Result result = shopService.queryById(13L);

        assertTrue(result.getSuccess());
        assertEquals(shop, result.getData());
        verify(bloomFilter, never()).mightContain(anyLong());
    }
}
