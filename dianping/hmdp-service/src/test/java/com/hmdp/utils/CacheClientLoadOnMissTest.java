package com.hmdp.utils;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CacheClientLoadOnMissTest {

    private StringRedisTemplate redisTemplate;
    private ValueOperations<String, String> valueOps;
    private CacheClient client;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisTemplate = mock(StringRedisTemplate.class);
        valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        // 平台线程模式（开关关）：本测试只验证 loadOnMiss 语义，与线程池实现无关
        client = new CacheClient(redisTemplate, false);
    }

    @Test
    void should_load_from_db_and_write_logical_expire_when_key_missing() {
        Shop shop = new Shop().setId(1L);
        when(valueOps.get("cache:shop:1")).thenReturn(null);
        when(valueOps.setIfAbsent("lock:shop:1", "1", 10, TimeUnit.SECONDS)).thenReturn(true);
        AtomicInteger dbCalls = new AtomicInteger();

        Shop result = client.loadOnMiss("cache:shop:", 1L, Shop.class,
                id -> {
                    dbCalls.incrementAndGet();
                    return shop;
                }, 30L, TimeUnit.MINUTES);

        assertSame(shop, result);
        assertEquals(1, dbCalls.get());
        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(valueOps).set(eq("cache:shop:1"), json.capture());
        RedisData written = JSONUtil.toBean(json.getValue(), RedisData.class);
        assertNotNull(written.getExpireTime());
        assertEquals(1L, JSONUtil.toBean((JSONObject) written.getData(), Shop.class).getId());
        verify(redisTemplate).delete("lock:shop:1");
    }

    @Test
    void should_write_negative_cache_when_db_missing() {
        when(valueOps.get("cache:shop:2")).thenReturn(null);
        when(valueOps.setIfAbsent("lock:shop:2", "1", 10, TimeUnit.SECONDS)).thenReturn(true);
        AtomicBoolean dbCalled = new AtomicBoolean();

        Shop result = client.loadOnMiss("cache:shop:", 2L, Shop.class,
                id -> {
                    dbCalled.set(true);
                    return null;
                }, 30L, TimeUnit.MINUTES);

        assertNull(result);
        assertTrue(dbCalled.get());
        verify(valueOps).set("cache:shop:2", "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
        verify(redisTemplate).delete("lock:shop:2");
    }

    @Test
    void should_return_null_without_db_when_negative_marker_exists() {
        when(valueOps.get("cache:shop:3")).thenReturn("");
        when(valueOps.setIfAbsent("lock:shop:3", "1", 10, TimeUnit.SECONDS)).thenReturn(true);
        AtomicBoolean dbCalled = new AtomicBoolean();

        Shop result = client.loadOnMiss("cache:shop:", 3L, Shop.class,
                id -> {
                    dbCalled.set(true);
                    return new Shop().setId(3L);
                }, 30L, TimeUnit.MINUTES);

        assertNull(result);
        assertFalse(dbCalled.get());
        verify(valueOps, never()).set(eq("cache:shop:3"), anyString());
        verify(redisTemplate).delete("lock:shop:3");
    }

    @Test
    void should_return_fresh_existing_value_without_db() {
        Shop shop = new Shop().setId(4L);
        RedisData cached = new RedisData();
        cached.setData(shop);
        cached.setExpireTime(LocalDateTime.now().plusMinutes(10));
        when(valueOps.get("cache:shop:4")).thenReturn(JSONUtil.toJsonStr(cached));
        when(valueOps.setIfAbsent("lock:shop:4", "1", 10, TimeUnit.SECONDS)).thenReturn(true);
        AtomicBoolean dbCalled = new AtomicBoolean();

        Shop result = client.loadOnMiss("cache:shop:", 4L, Shop.class,
                id -> {
                    dbCalled.set(true);
                    return null;
                }, 30L, TimeUnit.MINUTES);

        assertNotNull(result);
        assertEquals(4L, result.getId());
        assertFalse(dbCalled.get());
        verify(redisTemplate).delete("lock:shop:4");
    }

    @Test
    void should_rebuild_when_existing_value_is_logically_expired() {
        Shop staleShop = new Shop().setId(5L);
        RedisData stale = new RedisData();
        stale.setData(staleShop);
        stale.setExpireTime(LocalDateTime.now().minusMinutes(1));
        when(valueOps.get("cache:shop:5")).thenReturn(JSONUtil.toJsonStr(stale));
        when(valueOps.setIfAbsent("lock:shop:5", "1", 10, TimeUnit.SECONDS)).thenReturn(true);
        Shop freshShop = new Shop().setId(5L);
        AtomicInteger dbCalls = new AtomicInteger();

        Shop result = client.loadOnMiss("cache:shop:", 5L, Shop.class,
                id -> {
                    dbCalls.incrementAndGet();
                    return freshShop;
                }, 30L, TimeUnit.MINUTES);

        assertSame(freshShop, result);
        assertEquals(1, dbCalls.get());
        verify(valueOps).set(eq("cache:shop:5"), anyString());
        verify(redisTemplate).delete("lock:shop:5");
    }
}
