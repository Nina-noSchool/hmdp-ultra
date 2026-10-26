package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Redis 缓存工具 —— 封装缓存三大问题的解决方案。
 *
 * <p>提供两套写 + 三套读：
 * <ul>
 *   <li>{@link #set} — 普通写入（带 TTL 物理过期）</li>
 *   <li>{@link #setWithLogicalExpire} — 逻辑过期写入（不设置 Redis TTL，过期时间由 value 内部字段承载）</li>
 *   <li>{@link #queryWithPassThrough} — 读时防穿透（缓存空值 + 短期 TTL）</li>
 *   <li>{@link #queryWithLogicalExpire} — 读时防击穿（逻辑过期 + 互斥锁 + 独立线程重建 + 先返回旧值）</li>
 *   <li>{@link #loadOnMiss} — 冷 key 同步回源（互斥锁 + 逻辑过期写入 + 空值标记）</li>
 * </ul>
 *
 * <p>注：本类是 CacheClient 工具版，与 ShopServiceImpl 中那段被注释掉的"原始写法"功能等价，
 * 区别在于把 id + keyPrefix 这种业务参数泛化了，可复用到任意实体缓存。</p>
 *
 * @author 虎哥
 */
@Slf4j
@Component
public class CacheClient {

    /** Redis 操作模板（由 Spring 自动注入） */
    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 缓存重建线程池 —— 逻辑过期模式下，重建缓存是耗时 IO 操作，
     * 不能阻塞业务请求线程，所以放到独立线程池异步做。
     * 虚拟线程开关（docs/VIRTUAL-THREADS.md）开启时切换为 per-task executor；
     * 关闭时维持固定 10 线程历史行为。
     */
    private final ExecutorService cacheRebuildExecutor;

    public CacheClient(StringRedisTemplate stringRedisTemplate,
            @Value("${concurrency.virtual-threads.enabled:false}") boolean virtualThreadsEnabled) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.cacheRebuildExecutor = virtualThreadsEnabled
                ? Executors.newVirtualThreadPerTaskExecutor()
                : Executors.newFixedThreadPool(10);
    }

    /**
     * 普通缓存写入 —— 带 Redis 物理 TTL。
     *
     * <p>Redis 到期自动删除。适用：① 可容忍短暂不一致的普通缓存；
     * ② 配合 queryWithPassThrough 使用（空值也走这个写，只是 TTL 短）。</p>
     *
     * @param key   缓存键（调用方要自己拼好，比如 CACHE_SHOP_KEY + id）
     * @param value 缓存值（对象会被 JSON 序列化）
     * @param time  过期时间
     * @param unit  时间单位
     */
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /**
     * 逻辑过期写入 —— 不设 Redis TTL，在 value 里嵌一个 expireTime 字段。
     *
     * <p>数据结构：RedisData { data: 原始对象, expireTime: LocalDateTime }。
     * Redis 永不删除这份数据，靠读的时候判断 expireTime 是否过期来决定是否触发重建。</p>
     *
     * <p>适用：热点 key 防击穿。物理 TTL 容易导致"大量热点同时过期 → 全部打到 DB"，
     * 逻辑过期让过期时刻变得可控——过期后仍能返回旧值，后台异步重建。</p>
     *
     * @param key   缓存键
     * @param value 缓存值（会被包进 RedisData）
     * @param time  逻辑过期时长
     * @param unit  时间单位
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 缓存穿透防御 —— 缓存空值 + 短期 TTL。
     *
     * <p>穿透场景：请求一个 DB 里根本不存在的 id，Redis 没有 → 每次都打到 DB。
     * 解法：DB 查不到也把 "" 写入 Redis，且 TTL 很短（{@link RedisConstants#CACHE_NULL_TTL}）。</p>
     *
     * <p>判断三层含义：
     * <ol>
     *   <li>{@code json 非 blank} → 缓存命中且不是空值，直接返回</li>
     *   <li>{@code json != null} → 缓存命中但内容是 ""（被空值标记过），返回 null</li>
     *   <li>以上都不满足 → Redis 真没这个 key，去 DB 查，查不到就缓存 ""，查到就正常写缓存</li>
     * </ol>
     *
     * @param keyPrefix   缓存键前缀（如 "cache:shop:"）
     * @param id          业务主键
     * @param type        缓存值的目标类型（反序列化用）
     * @param dbFallback  DB 回退查询函数（方法引用，如 this::getById）
     * @param time        缓存正常过期时长
     * @param unit        时间单位
     * @param <R>         返回类型
     * @param <ID>        主键类型
     * @return            缓存/DB 查到的对象，或 null（表示穿透标记/DB 真没有）
     */
    public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // ① 先查 Redis
        String json = stringRedisTemplate.opsForValue().get(key);

        // ② 命中非空值 → 正常缓存命中，直接反序列化返回
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }

        // ③ json != null 但 isBlank → 命中了空字符串 ""
        //    这就是穿透保护生效：之前有人查过这个不存在的 id，
        //    我们把 "" 写进 Redis 并给了短 TTL，所以现在直接返回 null 挡掉穿透
        if (json != null) {
            return null;
        }

        // ④ json == null → Redis 里根本没这个 key（既不是正常数据也不是空值标记）
        //    真·没缓存过，去 DB 查
        R r = dbFallback.apply(id);

        // ⑤ DB 也没 → 缓存空值挡穿透，TTL 必须短（CACHE_NULL_TTL=2分钟），
        //    防止真的"新插入一条同名记录"后旧空值还挡着
        if (r == null) {
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        // ⑥ DB 查到了 → 写 Redis（走普通 set，带物理 TTL）+ 返回
        this.set(key, r, time, unit);
        return r;
    }

    /**
     * 缓存击穿防御 —— 逻辑过期 + 互斥锁 + 异步重建 + 先返回旧值。
     *
     * <p>击穿场景：某个热点 key 突然失效，大量并发同时打到 DB。
     * 本方法分四种情况：
     * <ol>
     *   <li>Redis key 不存在（冷启动/删缓存后）→ 同步 {@link #loadOnMiss} 回源 DB，
     *       查到写逻辑过期结构，查不到写空值标记</li>
     *   <li>Redis 是空值标记 "" → 直接返回 null（负缓存，短 TTL 内挡掉重复请求）</li>
     *   <li>Redis 查到且逻辑时间未过期 → 直接返回，正常命中</li>
     *   <li>Redis 查到但已过期 → 尝试加互斥锁：
     *       拿到锁 → 提交异步线程重建缓存（重建完释放锁）；
     *       没拿到锁 → 说明已有别的线程在重建，什么都不做
     *       两种情况都「先把这份过期数据返回给调用方」——这是逻辑过期的核心价值：
     *       不阻塞用户，用短暂脏读换高可用</li>
     * </ol>
     *
     * @param keyPrefix   缓存键前缀
     * @param id          业务主键
     * @param type        缓存值的目标类型
     * @param dbFallback  DB 回退查询函数
     * @param time        逻辑过期时长（写入 RedisData.expireTime 的值）
     * @param unit        时间单位
     * @param <R>         返回类型
     * @param <ID>        主键类型
     * @return            缓存对象；过期时可能返回旧数据（由调用方自行接受短暂脏读）
     */
    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // ① 查 Redis —— key 不存在（冷启动/删缓存后）走同步回源；空值标记走负缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        if (json == null) {
            return loadOnMiss(keyPrefix, id, type, dbFallback, time, unit);
        }
        if (StrUtil.isBlank(json)) {
            return null;
        }

        // ② 反序列化两层：先外层 RedisData（拿 expireTime），再内层实际数据
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R data = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        // ③ 逻辑时间没过期 → 正常命中，直接返回
        if (expireTime.isAfter(LocalDateTime.now())) {
            return data;
        }

        // ④ 已过期 → 尝试互斥锁，保证只有一个线程去重建，
        //    其他并发线程走"没拿到锁"分支，什么都不做直接走最后的 return
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if (isLock) {
            // ⑤ 拿到锁 → 异步线程重建（不阻塞当前业务线程）
            cacheRebuildExecutor.submit(() -> {
                try {
                    R newData = dbFallback.apply(id);
                    this.setWithLogicalExpire(key, newData, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(lockKey);
                }
            });
        }

        // ⑥ 无论拿到锁还是没拿到，都把"这份已过期的旧数据"先返回给用户
        //    —— 这是逻辑过期 vs 互斥锁重建的核心区别：
        //       互斥锁方案没拿到锁的线程会 sleep 重试（阻塞），
        //       逻辑过期方案没拿到锁的线程直接返回旧值（短暂脏读换高可用）
        return data;
    }

    /**
     * 冷 key 同步回源 —— key 不存在（冷启动/删除缓存后）时，用互斥锁保护 DB 查询，
     * 查到写逻辑过期结构，查不到写空值标记（短 TTL）。
     *
     * <p>为什么单独抽出来：逻辑过期模板原来对 blank 直接返回 null，前提是
     * "所有 key 都已预热"。一旦 update/评分刷新删掉 key，或冷启动从未写过，
     * 这个前提就不成立，用户会看到"店铺不存在"。本方法把"不存在"当成
     * 一次真正的缓存 miss，回源 DB 后重新建立缓存。</p>
     *
     * <p>锁与 queryWithLogicalExpire 共用 lock:shop:{id}，所以冷 key 回源和
     * 过期重建不会同时打 DB。抢不到锁时短暂重读 Redis，超时后兜底查一次 DB
     * （不写缓存），保证这次请求能拿到数据。</p>
     */
    public <R, ID> R loadOnMiss(String keyPrefix, ID id, Class<R> type,
                                Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;

        // 最多等 20 × 50ms = 1 秒，超时兜底直接查 DB
        for (int attempt = 0; attempt < 20; attempt++) {
            if (tryLock(lockKey)) {
                try {
                    // 锁内二次检查：拿到锁后先查一次 Redis，
                    // 可能别的线程已经重建完了（避免重复查 DB + 覆盖新数据）
                    String current = stringRedisTemplate.opsForValue().get(key);
                    if (current != null) {
                        if (StrUtil.isBlank(current)) {
                            // 空值标记：之前已经穿透防护过，直接返回 null
                            return null;
                        }
                        R fresh = readFresh(current, type);
                        if (fresh != null) {
                            // 缓存里有未过期数据，直接读走，不碰 DB
                            return fresh;
                        }
                        // 缓存存在但已逻辑过期 / 格式坏掉：拿到锁就顺手重建
                    }

                    // ── 真正回源 DB ──
                    R data = dbFallback.apply(id);
                    if (data == null) {
                        // DB 查不到 → 写空值缓存（防穿透）
                        try {
                            stringRedisTemplate.opsForValue().set(
                                    key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                        } catch (Exception e) {
                            log.warn("loadOnMiss negative-cache write failed, key={}", key, e);
                        }
                        return null;
                    }
                    // DB 有数据 → 写 Redis（带逻辑过期）
                    try {
                        this.setWithLogicalExpire(key, data, time, unit);
                    } catch (Exception e) {
                        log.warn("loadOnMiss cache write failed, key={}", key, e);
                    }
                    return data;
                } finally {
                    // 保证锁一定释放，unLock 自身异常不影响主流程
                    try {
                        unLock(lockKey);
                    } catch (Exception e) {
                        log.warn("loadOnMiss unlock failed, lockKey={}", lockKey, e);
                    }
                }
            }

            // ── 没拿到锁：别的线程正在回源 ──
            // 短暂 sleep 让 CPU 不忙等，随后重读 Redis 看别人有没有写完
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                // 被中断 → 恢复中断标记 + 直接回源 DB（不写缓存，保证这次请求拿到结果）
                Thread.currentThread().interrupt();
                return dbFallback.apply(id);
            }
            // 睡眠后重读 Redis：如果锁内线程已经写完，这里能直接读到
            String current = stringRedisTemplate.opsForValue().get(key);
            if (current != null) {
                if (StrUtil.isBlank(current)) {
                    return null;
                }
                R fresh = readFresh(current, type);
                if (fresh != null) {
                    return fresh;
                }
                // 读到了但还是过期/坏掉 → 下一轮循环继续抢锁
            }
        }

        // 20 次循环后还没拿到锁 → 锁竞争超时
        // 不写缓存，直接回源一次 DB，保证这次请求总能拿到数据
        return dbFallback.apply(id);
    }

    /** 从已存在的逻辑过期结构里读未过期的数据；空值/过期/格式异常都返回 null。 */
    private <R> R readFresh(String json, Class<R> type) {
        if (StrUtil.isBlank(json)) {
            return null;
        }
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        if (redisData.getExpireTime() == null || !redisData.getExpireTime().isAfter(LocalDateTime.now())) {
            return null;
        }
        return JSONUtil.toBean((JSONObject) redisData.getData(), type);
    }

    /**
     * 简单互斥锁（SETNX + 10 秒自动过期）。
     *
     * <p>注意：这把锁是为"互斥锁重建缓存"场景设计的轻量版本，
     * 非 Redisson 那种可重入/续期锁。10 秒过期是硬编码的兜底——
     * 正常情况下重建缓存几毫秒就完，10 秒足够避免死锁。</p>
     *
     * @param key 锁键（如 "lock:shop:123"）
     * @return true = 拿到锁；false = 锁已被别人持有
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁 —— 直接删除 key。
     *
     * <p>注意：没做"比对 value 再删"（防误删别人的锁），
     * 因为 tryLock 的 value 固定写死 "1" 没有唯一标识。
     * 这个简化在 CacheClient 的单 JVM 重建场景下够用。</p>
     *
     * @param key 锁键
     */
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }
}