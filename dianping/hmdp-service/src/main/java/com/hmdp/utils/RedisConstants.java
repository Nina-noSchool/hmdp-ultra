package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;
    /** 登录设备指针：login:device:{userId}:{deviceId} → 该设备当前生效的 token（设备互踢锚点） */
    public static final String LOGIN_DEVICE_KEY = "login:device:";

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";
    public static final String SHOP_RATING_REFRESH_LOCK_KEY = "lock:shop:rating:refresh:";
    public static final String SHOP_RATING_REFRESH_TS_KEY = "shop:rating:refresh:ts:";
    public static final String SHOP_HOT_KEY = "shop:hot:";
    public static final Long SHOP_RATING_REFRESH_TTL_MINUTES = 30L;

    public static final String SHOP_TYPE_KEY = "shop_type:";
    public static final Long SHOP_TYPE_LONG=10L;

    /** 秒杀入口滑动窗口限流：rate:sw:seckill:{userId}（批次1 ①） */
    public static final String SECKILL_RATE_LIMIT_KEY = "rate:sw:seckill:";
}
