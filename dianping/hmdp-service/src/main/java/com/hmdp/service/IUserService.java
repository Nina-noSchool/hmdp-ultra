package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.User;

import jakarta.servlet.http.HttpSession;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IUserService extends IService<User> {

    Result sendCode(String phone, HttpSession session);

    /**
     * 登录并绑定设备：同设备重复登录时原子踢掉旧会话（设备互踢）。
     *
     * @param loginForm 登录参数（手机号 + 验证码）
     * @param session   HttpSession（保留兼容签名，会话数据实际存 Redis）
     * @param deviceId  客户端设备标识（请求头 X-Device-Id），空白时归一化为 default
     * @return 登录结果，data 为本次登录的 token
     */
    Result login(LoginFormDTO loginForm, HttpSession session, String deviceId);

    /**
     * 登出：删除当前 token 的会话 Hash，会话立即失效。
     *
     * @param token 请求头 authorization 携带的登录令牌
     */
    void logout(String token);

    Result sign();

    Result signCount();
}
