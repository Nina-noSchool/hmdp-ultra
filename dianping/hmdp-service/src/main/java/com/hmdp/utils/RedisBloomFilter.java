package com.hmdp.utils;

import cn.hutool.core.lang.Assert;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于 Redis BitMap 的分布式布隆过滤器（手写实现，不依赖 hutool/Redisson）。
 *
 * <p>设计要点（与"JVM 本地布隆"的关键差异，也是面试对比点）：</p>
 * <ul>
 *   <li><b>分布式一致</b>：位图存在 Redis 里，多实例共享同一份，重启不丢失；</li>
 *   <li><b>参数显式可算</b>：期望插入量 n + 目标误判率 p → 最优位数 m = -n·ln(p)/(ln2)²
 *       （向上取 2 的幂以便位掩码取模）、最优哈希数 k = (m/n)·ln2；</li>
 *   <li><b>双重哈希</b>：FNV-1a 64 位得 h1，splitmix64 终化得奇数 h2，
 *       第 i 个位偏移 = (h1 + i·h2) mod m（Kirsch-Mitzenmacher，只需两个基础哈希）；</li>
 *   <li><b>Lua 原子</b>：k 次 GETBIT / k 次 SETBIT 合并为一次脚本调用；</li>
 *   <li><b>fail-open</b>：未初始化（rebuild 未完成）或 Redis 异常时一律放行，
 *       行为退化为"没有这层守卫"——守卫只能锦上添花，不能成为新的故障点；
 *       布隆说"可能存在"就走原有 负缓存/DB 链路，误判（把不存在的当存在）只浪费一次查询，绝不误杀真实数据。</li>
 * </ul>
 *
 * <p>一致性边界：布隆只能 add 不能 remove（互相置位），误判率随元素增长单调上升；
 * 通过定时 {@link #resync} 全量重同步归零。若将来新增店铺创建入口，须在创建成功后调用 {@link #add}。</p>
 */
@Slf4j
public class RedisBloomFilter {

    private final StringRedisTemplate template;
    private final String bitmapKey;
    /** 位图大小（2 的幂），实际分配位数为理论最优值向上取整 */
    private final long bits;
    /** 位掩码 = bits - 1，用位与代替取模 */
    private final long bitMask;
    /** 哈希函数个数 k */
    private final int hashes;
    /** 初始化开关：rebuild 完成前 fail-open */
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    /** 重建互斥：防止启动 Runner 与定时 Job 并发重建 */
    private final AtomicBoolean rebuilding = new AtomicBoolean(false);

    private static final DefaultRedisScript<Long> ADD_SCRIPT;
    private static final DefaultRedisScript<Long> CONTAINS_SCRIPT;

    static {
        ADD_SCRIPT = new DefaultRedisScript<>();
        ADD_SCRIPT.setLocation(new ClassPathResource("bloom_add.lua"));
        ADD_SCRIPT.setResultType(Long.class);
        CONTAINS_SCRIPT = new DefaultRedisScript<>();
        CONTAINS_SCRIPT.setLocation(new ClassPathResource("bloom_contains.lua"));
        CONTAINS_SCRIPT.setResultType(Long.class);
    }

    public RedisBloomFilter(StringRedisTemplate template, String bitmapKey,
                            long expectedInsertions, double fpp) {
        Assert.isTrue(expectedInsertions > 0, "expectedInsertions must be positive");
        Assert.isTrue(fpp > 0 && fpp < 1, "fpp must be in (0,1)");
        this.template = template;
        this.bitmapKey = bitmapKey;
        // 最优位数 m = -n·ln(p) / (ln2)²，向上取 2 的幂（位数只增不减，误判率只会更低）
        double optimalBits = -expectedInsertions * Math.log(fpp) / (Math.log(2) * Math.log(2));
        long bits = Long.highestOneBit((long) optimalBits) << 1;
        this.bits = bits;
        this.bitMask = bits - 1;
        // 最优哈希数 k = (m/n)·ln2（按取幂前的理论 m 计算，避免取整放大）
        this.hashes = Math.max(1, (int) Math.round(optimalBits / expectedInsertions * Math.log(2)));
        log.info("bloom filter init: key={}, expectedInsertions={}, fpp={}, bits={}, hashes={}",
                bitmapKey, expectedInsertions, fpp, bits, hashes);
    }

    /**
     * 可能存在检查。未初始化或 Redis 异常时 fail-open 返回 true（放行走原有缓存/DB 链路）。
     * 返回 true 不代表一定存在（误判率 = fpp），返回 false 则一定不存在。
     */
    public boolean mightContain(long id) {
        if (!initialized.get()) {
            return true;
        }
        try {
            Long result = executeContains(id);
            return result == null || result != 0L;
        } catch (Exception e) {
            log.warn("bloom contains failed, fail-open, key={}, id={}", bitmapKey, id, e);
            return true;
        }
    }

    /** 写入一个元素（k 个位全部置 1）。新建业务数据时必须调用，否则会 false negative 误杀。 */
    public void add(long id) {
        executeAdd(id);
    }

    /** Lua 包含性检查钩子（包级可见，测试用内存位图覆写） */
    Long executeContains(long id) {
        return template.execute(CONTAINS_SCRIPT,
                Collections.singletonList(bitmapKey), offsets(id));
    }

    /** Lua 写入钩子（包级可见，测试用内存位图覆写） */
    void executeAdd(long id) {
        template.execute(ADD_SCRIPT, Collections.singletonList(bitmapKey), offsets(id));
    }

    /**
     * 全量重同步：清空位图后重新写入全量 id。
     * 重建期间 initialized=false（fail-open 放行），完成后恢复守卫；
     * 通过 rebuilding 标志保证同一时刻只有一次重建（启动 Runner 与定时 Job 竞争时后者直接跳过）。
     * 重建中途失败会停留在 fail-open 状态，等下一次重建恢复——宁可退化也不误杀。
     */
    public void resync(long[] ids) {
        if (!rebuilding.compareAndSet(false, true)) {
            log.info("bloom resync already in progress, skip, key={}", bitmapKey);
            return;
        }
        try {
            initialized.set(false);
            template.delete(bitmapKey);
            for (long id : ids) {
                executeAdd(id);
            }
            initialized.set(true);
            log.info("bloom filter resynced: key={}, ids={}", bitmapKey, ids.length);
        } finally {
            rebuilding.set(false);
        }
    }

    /** 是否已完成重建（fail-open 检查用） */
    public boolean isInitialized() {
        return initialized.get();
    }

    /** 位图大小（测试断言用） */
    long bits() {
        return bits;
    }

    /** 哈希函数个数（测试断言用） */
    int hashFunctions() {
        return hashes;
    }

    /**
     * 计算一个 id 对应的 k 个位偏移（包级可见，供测试复算校验）。
     * 双重哈希：offset_i = (h1 + i·h2) & bitMask，h2 为奇数保证 i 遍历时有足够周期。
     */
    long[] bitOffsets(long id) {
        long h1 = fnv1a64(Long.toString(id));
        long h2 = mix64(h1) | 1L;
        long[] offsets = new long[hashes];
        for (int i = 0; i < hashes; i++) {
            offsets[i] = (h1 + (long) i * h2) & bitMask;
        }
        return offsets;
    }

    /** FNV-1a 64 位哈希 */
    private static long fnv1a64(String s) {
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < s.length(); i++) {
            hash ^= s.charAt(i);
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    /** splitmix64 终化混合（让 h1 的任意两位翻转都雪崩式影响输出） */
    private static long mix64(long z) {
        z ^= (z >>> 33);
        z *= 0xff51afd7ed558ccdL;
        z ^= (z >>> 33);
        z *= 0xc4ceb9fe1a85ec53L;
        z ^= (z >>> 33);
        return z;
    }

    /** 偏移量转字符串参数（Lua GETBIT/SETBIT 直接接受整数字符串） */
    private Object[] offsets(long id) {
        long[] offsets = bitOffsets(id);
        String[] args = new String[offsets.length];
        for (int i = 0; i < offsets.length; i++) {
            args[i] = String.valueOf(offsets[i]);
        }
        return args;
    }

    /** 供 rebuild 的流式调用（List&lt;Long&gt; → long[]） */
    public static long[] toLongArray(List<Long> ids) {
        long[] arr = new long[ids.size()];
        for (int i = 0; i < ids.size(); i++) {
            arr[i] = ids.get(i);
        }
        return arr;
    }
}
