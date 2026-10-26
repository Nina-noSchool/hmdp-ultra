package com.hmdp.shoprating.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.shoprating.domain.model.ShopRatingRefreshMessage;
import com.hmdp.shoprating.domain.model.ShopRatingTriggerSource;
import com.hmdp.shoprating.service.ShopRatingRefreshService;
import com.hmdp.utils.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class ShopRatingRefreshServiceImpl implements ShopRatingRefreshService {

    /** RocketMQ 目的地: topic=shop-rating-refresh-topic, tag=refresh */
    private static final String SHOP_RATING_REFRESH_DESTINATION = "shop-rating-refresh-topic:refresh";

    /** RocketMQ 模板: 用于发送店铺热度评分刷新消息, listener 消费后会跑 shop_rating_refresh 任务链 */
    @Resource
    private RocketMQTemplate rocketMQTemplate;

    /** Redis 字符串模板: 存储每个店铺上次评分刷新的时间戳, 做过期判断 */
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /** Redisson 分布式锁客户端: 多实例部署时防止同一店铺同时发多条刷新 MQ */
    @Resource
    private RedissonClient redissonClient;

    /**
     * 探店博客发布触发评分刷新 (即时触发, 无去重)
     * <p>
     * 触发源: 用户发布探店博客后, BlogService 调用此方法。
     * 与 triggerIfExpired 的区别:
     * <ul>
     *   <li>博客发布是<b>明确的写操作事件</b>, 必然影响店铺热度, 所以<b>直接发 MQ 不做去重</b></li>
     *   <li>商详查询是<b>读操作</b>, 高 QPS 下同一个店铺每秒可能被查几百次, 必须用 Redis 时间戳 + 分布式锁做<b>双重去重</b></li>
     * </ul>
     * </p>
     *
     * @param shopId 被探店的店铺 ID
     */
    @Override
    public void triggerFromBlog(Long shopId) {
        ShopRatingRefreshMessage message = new ShopRatingRefreshMessage(
                shopId, ShopRatingTriggerSource.BLOG_PUBLISHED, System.currentTimeMillis());
        rocketMQTemplate.syncSend(SHOP_RATING_REFRESH_DESTINATION,
                org.springframework.messaging.support.MessageBuilder.withPayload(JSONUtil.toJsonStr(message)).build());
        log.info("shop rating refresh message sent from blog, shopId={}", shopId);
    }

    /**
     * 商详查询后检查是否需要触发评分刷新 (双重去重: Redis 时间戳 + Redisson 分布式锁)
     * <p>
     * 触发源: ShopDetailApplicationService.getShopDetail() 返回商详结果后异步调用。
     * 为什么需要去重?
     * <pre>
     * 商详接口 QPS = 1000, 店铺数 = 100
     * → 平均每个店铺每秒被查 10 次
     * → 如果每次查都发 MQ, 每分钟每个店铺发 600 条评分刷新消息
     * → 评分刷新任务链本身很重 (聚合多维度数据), 会被打爆
     * </pre>
     * 所以这里用了经典的<b>双重检查锁 (Double-Checked Locking)</b> 模式:
     * <ol>
     *   <li><b>第一次检查 (无锁, 快速路径)</b> —— 查 Redis 时间戳, 如果距离上次刷新还没超过 TTL 分钟, 直接 return</li>
     *   <li><b>获取分布式锁</b> —— tryLock 非阻塞, 拿不到锁说明其他实例已经在处理了, 直接 return</li>
     *   <li><b>第二次检查 (有锁, 慢速路径)</b> —— 拿到锁后再查一遍 Redis 时间戳, 防止多线程同时通过了第一次检查后重复发送</li>
     *   <li><b>发送 MQ</b> —— 只有通过两次检查的那个线程才能真正发消息</li>
     * </ol>
     * </p>
     * <p>
     * 注意: Redis 时间戳的<b>更新不在这个方法里</b>, 而是在 MQ listener 消费完评分刷新任务链后写回 Redis。
     * 这样做的原因是: 如果发了 MQ 但 listener 执行失败, 下次商详查询还能触发刷新, 不会因为时间戳提前更新而"漏刷"。
     * </p>
     *
     * @param shopId 刚被查询过的店铺 ID
     */
    @Override
    public void triggerIfExpired(Long shopId) {
        // ======= 第一次检查 (无锁, 快速路径) =======
        // Redis key = "shop:rating:refresh:ts:{shopId}", value = 上次刷新的时间戳 (毫秒)
        String key = RedisConstants.SHOP_RATING_REFRESH_TS_KEY + shopId;
        String value = stringRedisTemplate.opsForValue().get(key);
        if (value != null
                && System.currentTimeMillis() - Long.parseLong(value)
                   < TimeUnit.MINUTES.toMillis(RedisConstants.SHOP_RATING_REFRESH_TTL_MINUTES)) {
            // 距离上次刷新还没超过 TTL 分钟, 不需要重复触发
            return;
        }

        // ======= 获取分布式锁 =======
        // Redis key = "shop:rating:refresh:lock:{shopId}", Redisson 实现的公平锁
        RLock lock = redissonClient.getLock(RedisConstants.SHOP_RATING_REFRESH_LOCK_KEY + shopId);
        if (!lock.tryLock()) {
            // 锁已被其他实例持有, 说明它正在做第二次检查 + 发 MQ, 这里放弃
            return;
        }
        try {
            // ======= 第二次检查 (有锁, 慢速路径) =======
            // 拿到锁后必须再查一遍 —— 多个并发线程可能都同时通过了第一次无锁检查,
            // 但只有一个拿到了锁, 其他人在 tryLock 处返回了
            // 拿到锁的这个线程需要确认: 在它等锁的这段时间里, 是不是已经有人发过 MQ 了
            value = stringRedisTemplate.opsForValue().get(key);
            if (value != null
                    && System.currentTimeMillis() - Long.parseLong(value)
                       < TimeUnit.MINUTES.toMillis(RedisConstants.SHOP_RATING_REFRESH_TTL_MINUTES)) {
                return;
            }

            // ======= 发送 MQ =======
            ShopRatingRefreshMessage message = new ShopRatingRefreshMessage(
                    shopId, ShopRatingTriggerSource.SHOP_DETAIL_EXPIRED, System.currentTimeMillis());
            rocketMQTemplate.syncSend(SHOP_RATING_REFRESH_DESTINATION,
                    org.springframework.messaging.support.MessageBuilder.withPayload(JSONUtil.toJsonStr(message)).build());
            log.info("shop rating refresh message sent from expired check, shopId={}", shopId);
        } finally {
            // 必须在 finally 里释放锁, 防止 try 块抛异常导致锁泄漏
            lock.unlock();
        }
    }
}