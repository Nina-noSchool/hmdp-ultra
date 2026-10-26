package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.shopreview.dto.CreateShopReviewRequest;
import com.hmdp.shopreview.service.ShopReviewRewardService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.MultiLevelCacheService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.Generated;
import jakarta.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient clientClient;

    /** 多级缓存服务（批次3 ②：L1 本地缓存叠加在逻辑过期之上） */
    @Resource
    private MultiLevelCacheService multiLevelCacheService;

    /** 本地缓存开关：false = 完全回退到单级 Redis 链路（批次3 之前的原行为） */
    @Value("${cache.local.enabled:true}")
    private boolean localCacheEnabled;

    @Resource
    private ShopReviewRewardService shopReviewRewardService;
    @Override
    public Result queryById(Long id){
        /*缓存穿透
        //Shop shop = queryWithPassThrough(id);
        //Shop shop = clientClient
               // .queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);
//        if(shop==null){
//            return Result.fail("店铺不存在！");
//        }
         */

        //用逻辑过期解决缓存击穿
        // Shop shop = queryWithLogicalExpire(id);
        // 批次3 ②：默认走多级缓存（L1 本地 → L2 Redis 逻辑过期 → DB）；开关关闭则完全回退单级
        Shop shop = localCacheEnabled
                ? multiLevelCacheService.queryWithLocalCache(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES)
                : clientClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if(shop==null){

            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);

    }

      /*   public Shop queryWithLogicalExpire(Long id)   {
//        //1.尝试从Redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
//        //2.判断缓存是否存在
//        if(StrUtil.isBlank(shopJson)) { //判断字符串既不为null，也不是空字符串(""),且也不是空白字符
//            //3.不存在，返回商铺信息
//            return null;
//
//        }
//
//        //4.存在，将json反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        //5.判断是否过期
//        if(expireTime.isAfter(LocalDateTime.now())) {
//            //5.1.未过期，直接返回店铺信息
//            return shop;
//        }
//        //5.2.已过期，需要返回缓存重建
//        //6.缓存重建
//        //6.1.获取互斥锁
//        String lockKey=RedisConstants.LOCK_SHOP_KEY+id;
//        boolean isLock = tryLock(lockKey);
//        //6.2.判断是否获取锁成功
//        if(isLock){
//            //  6.3.成功，开启独立线程实现缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(()->{
//                try {
//                    //重建缓存
//                    this.saveShop2Redis(id,20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }finally {
//                    //释放锁
//                    unLock(lockKey);
//                }
//            });
//
//        }
//
//        //6.4.返回过期的商铺信息
//        return shop;
//
//    }
    /**
     * 互斥锁解决缓存穿透
     * @param id
     * @return
     * @throws InterruptedException
     */
    /* public Shop queryWithMutex(Long id) throws InterruptedException {
        //1.尝试从Redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //2.判断缓存是否存在
        if(StrUtil.isNotBlank(shopJson)) { //判断字符串既不为null，也不是空字符串(""),且也不是空白字符
            //3.存在，返回商铺信息
            return JSONUtil.toBean(shopJson, Shop.class);

        }
        //判断是否为空值
        if(shopJson!=null){
            return null;
        }
        //4.实现缓存重建
        //4.1获取互斥锁
        String lockKey="lock:shop:"+id;
        Shop shop=null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2判断是否获取成功
            if(!isLock) {
                //4.3失败，则休眠重试
               Thread.sleep(50);
                return queryWithMutex(id);
            }

            //4.4.成功，根据id查询数据库
            shop = getById(id);
            //模拟重建的延迟
            Thread.sleep(200);
            //5.判断数据库中是否存在
            if(shop==null){
                //6.不存在，返回错误状态码
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            //7.存在，写入redis，返回商铺信息
            String newShopJson = JSONUtil.toJsonStr(shop);
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,newShopJson,RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            unLock(lockKey);
        }

        //9.返回
        return shop;

    }*/
    /**
     * 缓存穿透
     * @param id
     * @return
     */
    /*
//        //1.尝试从Redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
//        //2.判断缓存是否存在
//        if(StrUtil.isNotBlank(shopJson)) { //判断字符串既不为null，也不是空字符串(""),且也不是空白字符
//            //3.存在，返回商铺信息
//            return JSONUtil.toBean(shopJson, Shop.class);
//
//        }
//        //判断是否为空值
//        if(shopJson!=null){
//            return null;
//        }
//        //4.不存在，根据id查询数据库
//        Shop shop = getById(id);
//        //5.判断数据库中是否存在
//        if(shop==null){
//            //6.不存在，返回错误状态码
//            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
//            return null;
//        }
//        //7.存在，写入redis，返回商铺信息
//        String newShopJson = JSONUtil.toJsonStr(shop);
//        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,newShopJson,RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
//
//        return shop;
//
//    }

     */


    public void saveShop2Redis(Long id,Long expireSeconds){
        //1.查询店铺数据
        Shop shop = getById(id);
        //2.封装成逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id==null){
            return Result.fail("店铺id不能为空");
        }
        //1.先修改数据库
        updateById(shop);
        //2.删除缓存：本地+Redis 双删，并广播其他实例失效（批次3 ②）
        multiLevelCacheService.evictAndBroadcast(CACHE_SHOP_KEY, shop.getId());
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.是否根据坐标查询
        if(x==null||y==null){
            //不需要坐标查询，该数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        //2.计算分页参数
        int from=(current-1)*SystemConstants.DEFAULT_PAGE_SIZE;
        int end=current*SystemConstants.DEFAULT_PAGE_SIZE;

        //3.查询redis，按照距离排序，分页。结果：shopId,distance
        String key = SHOP_GEO_KEY + typeId;
        //在 Redis 中按地理坐标（x, y）查询距离当前用户位置 5000 米内的商店
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        if(results==null){
            return Result.ok(Collections.emptyList());
        }
        //4.解析出id
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size()<=from){
            return Result.ok(Collections.emptyList());
        }
        //4.1.截取从from到end部分  跳过前 from 个结果，实现分页
        List<Long> ids=new ArrayList<>(list.size());
        Map<String,Distance> distanceMap=new HashMap<>(list.size());
        list.stream().skip(from).forEach(result->{
            //4.2.获取店铺id
            String shopIdStr=result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr,distance);
        });
        //5.根据id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query()
                .in("id", ids).last("order by field(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        //6.返回
        return Result.ok(shops);
    }

    @Override
    public Result createShopReview(Long userId, CreateShopReviewRequest request) {
        return Result.ok(shopReviewRewardService.createReview(userId, request));
    }
}
