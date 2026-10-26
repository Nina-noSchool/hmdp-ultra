package com.hmdp.interceptor;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import jakarta.annotation.Resource;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Token 续期拦截器 —— 拦截器链最前端，对所有请求刷 Token 过期时间 + 向 ThreadLocal 注入登录用户。
 *
 * <p>拦截器链位置（由 {@link com.hmdp.config.MvcConfig} 注册，order 越小越先执行）：
 * <pre>{@code
 *   RefreshTokenInterceptor (order=0, 最先执行) → ★ 本拦截器，对 /** 生效
 *   LoginInterceptor       (order=1)           → 校验 UserHolder 里有没有用户
 *   SlidingWindowInterceptor(order=2)          → 秒杀入口限流
 * }</pre></p>
 *
 * <p>为什么排 order=0 且拦所有路径？
 * <ul>
 *   <li>对 <b>公开接口</b>（店铺列表、秒杀券列表、{@code /user/login} 等）也要跑 ——
 *       已登录用户浏览公开页面时 Token 不能断续，否则逛一圈回来就要重新登录；</li>
 *   <li>对 <b>需登录接口</b>，本拦截器先把 UserDTO 塞进 {@code UserHolder}(ThreadLocal)，
 *       后续 LoginInterceptor(order=1) 才能拿到非空值放行 —— 两者通过 ThreadLocal 隐式协作。</li>
 * </ul></p>
 *
 * <p>Token 生命周期：登录时写入 Redis Hash（key={@code login:user:}{token}），
 * 默认有效期 30 分钟。本拦截器每次命中有效 Token 都会续 30 分钟，实现"活跃用户永不过期、
 * 沉默 30 分钟后自动退出"的语义。</p>
 *
 * <p>注册方式：本类没有标注 {@code @Component}，由 {@code MvcConfig} 手动
 * {@code new RefreshTokenInterceptor(stringRedisTemplate)} 实例化并注册 ——
 * 因为需要对 {@code /**} 生效，而 {@link LoginInterceptor} 是通过
 * {@code excludePathPatterns} 排除公开接口的，两者的注册模式不同。</p>
 */
public class RefreshTokenInterceptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * Token 续期 + ThreadLocal 注入。
     *
     * <p>三条放行路径，语义各不同：
     * <ul>
     *   <li><b>请求无 Token</b> → 放行但不塞 ThreadLocal（公开接口正常访问；需登录接口会被
     *       LoginInterceptor(order=1) 拦下返回 401）；</li>
     *   <li><b>Token 在 Redis 中已不存在</b>（过期 / 被主动踢下线） → 同样放行不塞 ThreadLocal，
     *       交给下游拦截器处理；</li>
     *   <li><b>Token 有效</b> → 塞 ThreadLocal + 续期 30 分钟 + 放行。</li>
     * </ul>
     * 三条路径都不终止拦截链（始终 return true），因为本拦截器不做鉴权决策，鉴权交给 LoginInterceptor。</p>
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 1. 从请求头取 Token —— 前端登录后每次请求都放在 "authorization" 字段
        String token = request.getHeader("authorization");
        // 2. 无 Token → 放行但不塞 ThreadLocal（公开接口正常访问，需登录接口交给 LoginInterceptor 拦）
        if (StrUtil.isBlank(token)) {
            return true;
        }
        String userKey = RedisConstants.LOGIN_USER_KEY + token;
        // 3. 从 Redis Hash 取用户数据（登录时以 token 为 key 写入，字段含 userId/nickname/avatar 等）
        Map<Object, Object> map = stringRedisTemplate.opsForHash().entries(userKey);
        // 4. Token 已过期 / 不存在 → 放行不塞 ThreadLocal，下游 LoginInterceptor 会返回 401
        if (map.isEmpty()) {
            return true;
        }
        // 5. Hash → UserDTO（只取业务字段，不暴露密码等敏感信息给下游）
        UserDTO userDTO = BeanUtil.fillBeanWithMap(map, new UserDTO(), false);
        // 6. 塞进 ThreadLocal —— 后续 LoginInterceptor、Controller 随时可取当前用户
        UserHolder.saveUser(userDTO);
        // 7. 刷新 Token 有效期 30 分钟 —— "活跃用户永不过期"语义的关键一步
        stringRedisTemplate.expire(userKey, 30, TimeUnit.MINUTES);
        return true;
    }

    /**
     * 请求完成后清理 ThreadLocal —— 防止 Tomcat 线程池复用时脏数据串到下一个请求。
     *
     * <p>Tomcat 工作线程是池化的：请求 A 用完线程后归还，请求 B 可能复用到同一个线程。
     * 如果不清理 ThreadLocal，请求 A 的 UserDTO 会"泄漏"到请求 B，导致 B 在未登录状态下
     * 拿到 A 的用户信息 —— 这是典型的线程池 + ThreadLocal 陷阱。</p>
     */
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
        HandlerInterceptor.super.afterCompletion(request, response, handler, ex);
    }
}