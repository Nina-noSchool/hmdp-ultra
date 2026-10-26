package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class RedisIdWorker {
    //开始时间戳
    private static final long BEGIN_TIMESTAMP=1640995200L;

    //序列号位数
    private static final int COUNT_BITS=32;

    //32位无符号上限
    private static final long MAX_SEQUENCE = 0xFFFFFFFFL;

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    public Long nextId(String keyPrefix){
        //1.生成时间戳 — 用系统时钟避免 LocalDateTime + ZoneOffset.UTC 与系统时间源不一致
        long nowSecond = System.currentTimeMillis() / 1000;
        long timestamp = nowSecond - BEGIN_TIMESTAMP;
        //2.生成序列号
        //2.1.获取当前日期，精确到天
        LocalDateTime now = LocalDateTime.now();
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);

        //2.2.越界保护：超过32位无符号上限时拒绝生成并记录日志
        if (count != null && count > MAX_SEQUENCE) {
            log.error("redis id sequence overflow, key={}, count={}, maxAllowed={}",
                    "icr:" + keyPrefix + ":" + date, count, MAX_SEQUENCE);
            throw new IllegalStateException("redis id sequence overflow for key: icr:" + keyPrefix + ":" + date);
        }

        //2.3.INCR 后设置 TTL（2天），防止每日 key 永久堆积
        stringRedisTemplate.expire("icr:" + keyPrefix + ":" + date, 2, TimeUnit.DAYS);

        //3.拼接并返回
        return timestamp<<COUNT_BITS | count;
    }

}
