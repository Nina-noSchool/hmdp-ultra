package com.hmdp.controller;


import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.entity.UserInfo;
import com.hmdp.service.IUserInfoService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

import jakarta.annotation.Resource;
import jakarta.servlet.http.HttpSession;

/**
 * 用户控制器
 * <p>
 * 提供用户相关的 HTTP 接口，包括发送验证码、登录/登出、查询用户信息、签到等功能。
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@RestController
@RequestMapping("/user")
public class UserController {

    /** 用户服务 */
    @Resource
    private IUserService userService;

    /** 用户详情服务 */
    @Resource
    private IUserInfoService userInfoService;

    /** Redis 模板 */
    @Autowired
    private StringRedisTemplate redisTemplate;

    /**
     * 发送手机验证码
     * <p>
     * 将生成的验证码存入 Redis（设置过期时间），并通过短信服务发送给用户。
     * </p>
     *
     * @param phone   用户手机号
     * @param session HttpSession
     * @return 发送结果
     */
    @PostMapping("code")
    public Result sendCode(@RequestParam("phone") String phone, HttpSession session) {
        return userService.sendCode(phone,session);
    }

    /**
     * 用户登录
     * <p>
     * 支持两种登录方式：手机号 + 验证码，或手机号 + 密码。登录成功后将用户信息保存到 Redis。
     * </p>
     *
     * @param loginForm 登录参数，包含手机号、验证码；或者手机号、密码
     * @param session   HttpSession
     * @return 登录结果，成功时返回用户信息
     */
    @PostMapping("/login")
    public Result login(@RequestBody LoginFormDTO loginForm, HttpSession session){
        return userService.login(loginForm,session);
    }

    /**
     * 用户登出
     * <p>
     * 清除当前线程中的用户信息占位。注：完整的登出功能还需移除 Redis 中的登录状态（待实现）。
     * </p>
     *
     * @return 登出结果
     */
    @PostMapping("/logout")
    public Result logout(){
        UserHolder.removeUser();
        return Result.fail("功能未完成");
    }

    /**
     * 获取当前登录用户信息
     * <p>
     * 从 {@link UserHolder} 中获取已登录用户的简要信息（脱敏后的 UserDTO）。
     * </p>
     *
     * @return 当前用户信息
     */
    @GetMapping("/me")
    public Result me(){
        UserDTO user = UserHolder.getUser();
        return Result.ok(user);
    }

    /**
     * 查询用户详情（个人资料）
     * <p>
     * 根据用户 ID 查询其扩展资料（如性别、生日等），首次查看可能无数据。
     * 返回时会屏蔽创建时间和更新时间字段。
     * </p>
     *
     * @param userId 用户 ID
     * @return 用户详情，或空结果
     */
    @GetMapping("/info/{id}")
    public Result info(@PathVariable("id") Long userId){
        UserInfo info = userInfoService.getById(userId);
        if (info == null) {
            return Result.ok();
        }
        info.setCreateTime(null);
        info.setUpdateTime(null);
        return Result.ok(info);
    }

    /**
     * 根据 ID 查询用户
     * <p>
     * 查询用户基础信息，并使用 Hutool 的 {@link BeanUtil} 将实体转换为 DTO 返回，
     * 避免敏感字段（如密码）泄露。
     * </p>
     *
     * @param userId 用户 ID
     * @return 用户简要信息，或空结果
     */
    @GetMapping("/{id}")
    public Result queryUserById(@PathVariable("id") Long userId){
        User user = userService.getById(userId);
        if(user==null){
            return Result.ok();
        }
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        return Result.ok(userDTO);
    }

    /**
     * 用户签到
     * <p>
     * 基于 Redis 的 Bitmap 实现每日签到，记录用户的签到状态。
     * </p>
     *
     * @return 签到结果
     */
    @PostMapping("/sign")
    public Result sign(){
        return userService.sign();
    }

    /**
     * 统计连续签到天数
     * <p>
     * 基于 Redis 的 Bitmap 回溯计算用户当前连续签到的天数。
     * </p>
     *
     * @return 连续签到天数
     */
    @GetMapping("/sign/count")
    public Result signCount(){
        return userService.signCount();
    }
}