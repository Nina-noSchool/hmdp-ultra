package com.hmdp.config;

import com.hmdp.interceptor.LoginInterceptor;
import com.hmdp.interceptor.RefreshTokenInterceptor;
import com.hmdp.interceptor.SlidingWindowInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import jakarta.annotation.Resource;

/**
 * Spring MVC 拦截器注册配置
 * <p>
 * 通过实现 {@link WebMvcConfigurer} 的 {@link #addInterceptors} 方法，
 * 向 Spring MVC 注册了三个拦截器，按照 order 值从小到大执行：
 * </p>
 * <ol>
 *   <li><b>RefreshTokenInterceptor</b>（order=0，最先执行）：拦截所有请求，刷新 Redis 中的 Token 过期时间，
 *       保证活跃用户不会因长时间未操作而掉线。</li>
 *   <li><b>LoginInterceptor</b>（order=1，其次执行）：校验当前请求是否已登录，未登录则拦截返回提示。
 *       通过 {@code excludePathPatterns} 放行无需登录即可访问的公开接口。</li>
 *   <li><b>SlidingWindowInterceptor</b>（order=2，登录之后执行）：对秒杀主链路按 userId 做滑动窗口限流，
 *       超限返回 429，保护入口 Redis 热点（批次1 ①）。</li>
 * </ol>
 * <p>
 * 两个拦截器分离的好处：公开接口虽然不需要登录，但依然需要刷新 Token（如果用户已登录），
 * 所以 RefreshTokenInterceptor 必须对 {@code /**} 生效且排在 LoginInterceptor 之前。
 * </p>
 */
@Configuration
public class MvcConfig implements WebMvcConfigurer {

    /** Redis 客户端，供 RefreshTokenInterceptor 查询 / 续期 Token 使用 */
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /** 秒杀入口滑动窗口限流拦截器（Spring 管理，带 @Value 配置项） */
    @Resource
    private SlidingWindowInterceptor slidingWindowInterceptor;

    /**
     * 注册拦截器
     * <p>
     * 注册顺序见类注释。order 值越小越先执行，这里 RefreshTokenInterceptor(order=0) → LoginInterceptor(order=1)。
     * </p>
     *
     * @param registry Spring 提供的拦截器注册表
     */
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // 登录拦截器：校验登录状态，排除公开路径
        registry.addInterceptor(new LoginInterceptor())
                .excludePathPatterns(
                        "/user/login",
                        "/upload/**",
                        "/voucher/**",
                        "/user/code",
                        "/shop/**",
                        "/shop-type/**",
                        "/blog/hot",
                        "/jvm/**"
                ).order(1);
        // 秒杀入口滑动窗口限流：只拦主链路 seckill-plus，排在登录拦截器之后（order=2）
        registry.addInterceptor(slidingWindowInterceptor)
                .addPathPatterns("/voucher-order/seckill-plus/**")
                .order(2);
        // Token 刷新拦截器：对所有请求刷新 Token 过期时间，必须排在登录拦截器之前
        registry.addInterceptor(new RefreshTokenInterceptor(stringRedisTemplate))
                .addPathPatterns("/**").order(0);
    }
}