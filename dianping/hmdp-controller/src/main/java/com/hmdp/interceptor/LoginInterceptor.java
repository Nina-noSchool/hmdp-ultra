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
 * 登录校验拦截器 —— 拦截需要登录的接口，未登录直接返回 HTTP 401。
 *
 * <p>在拦截器链中的位置（由 {@link com.hmdp.config.MvcConfig} 注册，order 越小越先执行）：
 * <pre>{@code
 *   RefreshTokenInterceptor (order=0) → 对所有请求刷 Token + 把 UserDTO 塞进 UserHolder(ThreadLocal)
 *   LoginInterceptor       (order=1) → ★ 本拦截器：校验 UserHolder 里有没有用户
 *   SlidingWindowInterceptor(order=2) → 秒杀入口限流（依赖 UserHolder 已有登录用户）
 * }</pre></p>
 *
 * <p>拦截规则：
 * <ul>
 *   <li>{@code UserHolder.getUser() == null} → 写 401 状态码 → 返回 false 终止拦截链，
 *       后续拦截器和 Controller 均不执行，前端可据此跳登录页；</li>
 *   <li>否则返回 true 放行。</li>
 * </ul>
 *
 * <p>公开接口（{@code /user/login}、店铺列表、秒杀券列表等）已通过
 * {@code MvcConfig.excludePathPatterns} 排除，不进入本拦截器。</p>
 *
 * <p>与 {@code RefreshTokenInterceptor} 的分工边界：
 * <ul>
 *   <li>RefreshTokenInterceptor 负责 <b>所有请求</b> 的 Token 续期 + 把用户放进 ThreadLocal，
 *       对公开接口也生效（已登录用户访问店铺列表也要续期）；</li>
 *   <li>本拦截器只拦需登录路径，<b>不做 Token 续期</b>，职责单一：
 *       UserHolder 里有值就过，没有就 401。</li>
 * </ul></p>
 *
 * <p>数据依赖链：本拦截器能拿到 UserDTO 的前提是上游 RefreshTokenInterceptor 已经跑过，
 * 按 order 顺序执行天然满足 —— 这也是为什么 RefreshToken 必须排 order=0 的原因。</p>
 */
public class LoginInterceptor implements HandlerInterceptor {

    /**
     * 登录校验 —— UserHolder 为空即视为未登录。
     *
     * <p>UserHolder 中的 UserDTO 由上游 {@code RefreshTokenInterceptor(order=0)} 提前写入
     * （从 Redis 查 Token → 解析 → UserHolder.saveUser），两者通过 ThreadLocal 衔接，
     * 没有显式方法调用。</p>
     *
     * @return true=放行给后续拦截器和 Controller；
     *         false=拦截链终止，Spring MVC 不再调用后续节点
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // UserHolder 数据来源：上游 RefreshTokenInterceptor(order=0) 从 Redis 取 Token 后塞入 ThreadLocal
        if (UserHolder.getUser() == null) {
            // 401 Unauthorized —— 未认证（区别于业务层 4xx 错误码，前端可据此跳登录页）
            response.setStatus(401);
            return false;
        }
        // 已登录，放行给 SlidingWindowInterceptor(order=2) 和 Controller
        return true;
    }
}