package com.hmdp.interceptor;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.metrics.MetricsRecorder;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import jakarta.annotation.Resource;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;

/**
 * 秒杀入口滑动窗口限流拦截器 —— 应用层第二道闸（第一道在网关/压测工具侧）。
 *
 * <p>注册在 {@link LoginInterceptor}（order=1）之后（order=2），
 * 保证拦截时 UserHolder 中已有登录用户；按 userId 维度用 Redis ZSET + Lua 做滑动窗口计数，
 * 超限直接返回 429，不进入秒杀主链路。</p>
 *
 * <p>为什么不用 Sentinel：当前是单机学习环境，自研轻量级实现便于把"窗口淘汰 + 计数 + 记录"
 * 的原子性讲清楚；限流算法与 Sentinel 的滑动窗口同构，是选型对照的现成弹药。</p>
 *
 * <p>全部参数可配（application.yaml）：
 * {@code seckill.rate-limit.sliding-window.enabled / window-ms / max-requests}。</p>
 */
@Slf4j
@Component
public class SlidingWindowInterceptor implements HandlerInterceptor {

    /** 滑动窗口限流 Lua 脚本（淘汰 → 计数 → 记录，单脚本原子完成） */
    private static final DefaultRedisScript<Long> SLIDING_WINDOW_SCRIPT;

    static {
        SLIDING_WINDOW_SCRIPT = new DefaultRedisScript<>();
        SLIDING_WINDOW_SCRIPT.setLocation(new ClassPathResource("lua/sliding_window.lua"));
        SLIDING_WINDOW_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /** 业务指标登记处（批次2 ③）：限流拒绝事件计数 */
    @Resource
    private MetricsRecorder metricsRecorder;

    /** 限流开关：压测对比实验用（开/关两组数据） */
    @Value("${seckill.rate-limit.sliding-window.enabled:true}")
    private boolean enabled;

    /** 窗口长度（毫秒），默认 1 秒 */
    @Value("${seckill.rate-limit.sliding-window.window-ms:1000}")
    private long windowMs;

    /** 窗口内最大请求数，默认 5 次 */
    @Value("${seckill.rate-limit.sliding-window.max-requests:5}")
    private int maxRequests;

    /**
     * 滑动窗口限流 —— 三条放行路径 + 一条拒绝路径。
     *
     * <p>执行顺序：开关检查 → 登录态检查 → Redis Lua 原子判定。
     * 限流逻辑全部下沉到 {@code sliding_window.lua}，本方法只负责组装参数、
     * 解释返回值、执行拒绝策略 —— Lua 原子性保证了高并发下窗口计数不会失真。</p>
     *
     * @return true=放行；false=限流拒绝（429 已写入 response，拦截链终止）
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 开关关闭 → 全放行，用于压测"限流开/关对照实验"
        if (!enabled) {
            return true;
        }
        UserDTO user = UserHolder.getUser();
        // 防御性 null 检查 —— 当前拦截器链中 LoginInterceptor(order=1) 先于本拦截器(order=2)执行，
        // 对 /voucher-order/seckill-plus/** 路径 Login 会检查 UserHolder 并在 null 时 return false
        // 终止拦截链，所以正常流程下能走到这里时 user 一定非空。此处仅为防止将来有人调整
        // Login 的 excludePathPatterns 或 order 时出现疏漏而兜底。
        if (user == null) {
            return true;
        }
        // 按 userId 隔离限流窗口 —— 每个用户有自己独立的 ZSET，互不影响
        String key = RedisConstants.SECKILL_RATE_LIMIT_KEY + user.getId();
        long now = System.currentTimeMillis();
        // member = 毫秒时间戳 + 8位随机串 —— 保证同一用户同一毫秒内的并发请求在 ZSET 里互不覆盖
        String member = now + "-" + UUID.randomUUID().toString().substring(0, 8);
        // Lua 原子执行：淘汰窗口外旧请求 → 计数 → 超限返回 0 / 放行则 ZADD + PEXPIRE
        Long result = stringRedisTemplate.execute(
                SLIDING_WINDOW_SCRIPT,
                Collections.singletonList(key),
                String.valueOf(now),
                String.valueOf(windowMs),
                String.valueOf(maxRequests),
                member
        );
        // Lua 返回 1 = 放行；null 或 0 = 被限流
        if (result != null && result == 1L) {
            return true;
        }
        log.warn("sliding window rate limit hit, userId={}, uri={}", user.getId(), request.getRequestURI());
        // 指标埋点：供 Prometheus/Grafana 看板观测限流强度（批次2 ③）
        metricsRecorder.incRatelimitRejected();
        writeTooManyRequests(response);
        return false;
    }

    /**
     * 写入 HTTP 429 Too Many Requests 响应。
     *
     * <p>用标准 429 状态码（语义明确：请求过多而非认证失败或业务错误），
     * 响应体套 {@link Result#fail} 壳保持与全局错误响应结构一致 ——
     * 前端/压测脚本可以同时按 HTTP 状态码（429 vs 200）和业务 code 区分
     * "限流拒绝"和"业务拒绝（库存不足/重复下单）"两种语义。</p>
     */
    private void writeTooManyRequests(HttpServletResponse response) throws Exception {
        response.setStatus(429);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.setContentType("application/json;charset=UTF-8");
        response.getWriter().write(JSONUtil.toJsonStr(Result.fail("请求过于频繁，请稍后再试")));
    }
}