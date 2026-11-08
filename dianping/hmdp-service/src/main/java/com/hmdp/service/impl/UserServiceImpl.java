package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import com.baomidou.mybatisplus.extension.conditions.query.LambdaQueryChainWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import jakarta.servlet.http.HttpSession;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.USER_SIGN_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    /** 会话有效期（分钟）：token Hash 与设备指针共用同一 TTL，活跃会话由拦截器滑动续期 */
    private static final long LOGIN_SESSION_TTL_MINUTES = 30L;

    /** 登录设备互踢 Lua（login_device_kick.lua）：单次网络往返原子完成「踢旧 token + 交换设备指针」 */
    private static final DefaultRedisScript<String> LOGIN_DEVICE_KICK_SCRIPT;

    static {
        LOGIN_DEVICE_KICK_SCRIPT = new DefaultRedisScript<>();
        LOGIN_DEVICE_KICK_SCRIPT.setLocation(new ClassPathResource("login_device_kick.lua"));
        LOGIN_DEVICE_KICK_SCRIPT.setResultType(String.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    /**
     * 发送验证码
     * @param phone
     * @param session
     * @return
     */
    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号
        if(RegexUtils.isPhoneInvalid(phone)) {
            //2.不符合，返回错误
            return Result.fail("手机号格式错误");
        }

        //3.符合，生成验证码
        String code = RandomUtil.randomNumbers(6);
        //4.保存验证码到redis
       stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY +phone,code,2, TimeUnit.MINUTES);
        //5.发送验证码
        log.info("短信验证码发送成功：{}",code);

        return Result.ok();

    }


    /**
     * 登录
     * @param loginForm
     * @param session
     * @return
     */
    /**
     * 登录并绑定设备。
     *
     * <p>会话模型：token → Redis Hash（{@code login:token:<token>}），另设
     * 设备指针（{@code login:device:{userId}:{deviceId}} → 当前生效 token）。
     * 同设备重复登录时，{@code login_device_kick.lua} 在一次 Redis 往返内
     * 原子完成「删旧会话 + 写新指针」——旧设备的下一次请求查不到会话即 401，
     * 这就是设备互踢；不同设备的指针互不干扰，天然支持多端共存。</p>
     *
     * <p>客户端通过请求头 {@code X-Device-Id} 声明设备；不传则归一化为
     * {@code default}，旧前端由此获得"同端互踢"语义。设备标识归一化后
     * 才允许拼进 Redis key（见 {@link #normalizeDeviceId}）。</p>
     *
     * <p>崩溃窗口说明：会话 Hash 先写、指针交换后执行，两步之间进程崩溃
     * 会留下"有会话、无指针"的孤儿会话——它受 30 分钟 TTL 约束自动消失，
     * 且属于同一 user+device，无越权面。</p>
     *
     * @param loginForm 登录参数
     * @param session   HttpSession（保留兼容签名，会话数据实际存 Redis）
     * @param deviceId  客户端设备标识（可空）
     * @return 登录结果，data 为本次登录的 token
     */
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session, String deviceId) {
        String code = loginForm.getCode();
        String phone = loginForm.getPhone();
        //1.校验手机号
        if(RegexUtils.isPhoneInvalid(phone)) {
            //2.不符合，返回错误
            return Result.fail("手机号格式错误");
        }
        //3.校验验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY+phone);
        if(cacheCode==null||!cacheCode.equals(code)){
           return Result.fail("验证码不一致，请重新输入");
       }

        //4.一致，根据手机号查询用户
        User user = query().eq("phone",phone).one();

        //5.判断用户是否存在

        //6.不存在，创建新用户，保存到数据库
        if(user==null){
           user=createUserWithPhone(phone);
        }
        //7.存在 保存到redis
        //7.1 生成个token作为登陆令牌
        String token = UUID.randomUUID().toString(true);

        //7.2 将user对象转为Hash存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> map = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        //7.3 记录会话所属设备（拦截器据此同步续期设备指针；
        //    压测脚本直写的 token 无此字段，热路径走原有两步、零额外开销）
        String safeDeviceId = normalizeDeviceId(deviceId);
        map.put("deviceId", safeDeviceId);
        //7.4 存储
        String tokenKey=RedisConstants.LOGIN_USER_KEY+token;
        stringRedisTemplate.opsForHash().putAll(tokenKey,map);
        stringRedisTemplate.expire(tokenKey, LOGIN_SESSION_TTL_MINUTES, TimeUnit.MINUTES);
        //7.5 设备互踢：原子交换设备指针，同设备旧会话立即失效
        String kicked = stringRedisTemplate.execute(LOGIN_DEVICE_KICK_SCRIPT,
                List.of(RedisConstants.LOGIN_DEVICE_KEY + userDTO.getId() + ":" + safeDeviceId),
                token, String.valueOf(TimeUnit.MINUTES.toSeconds(LOGIN_SESSION_TTL_MINUTES)));
        if (StrUtil.isNotEmpty(kicked)) {
            log.info("设备互踢生效：userId={} device={} 旧token={}", userDTO.getId(), safeDeviceId, kicked);
        }
        //8.返回token
        return Result.ok(token);
    }

    /**
     * 登出：删除当前 token 的会话 Hash，会话立即失效。
     *
     * <p>设备指针刻意不删：它最多再存活一个会话 TTL，且指向一个已不存在的 token ——
     * 下次同设备登录时互踢脚本会对它执行无害的 DEL，语义完全正确；反过来若在
     * 登出时清指针，会引入"登出与新登录并发时误删新会话指针"的竞态。</p>
     */
    @Override
    public void logout(String token) {
        stringRedisTemplate.delete(RedisConstants.LOGIN_USER_KEY + token);
    }

    /**
     * 归一化客户端设备标识，保证可以安全拼进 Redis key。
     *
     * <p>空白 → {@code default}；仅含字母数字与 {@code . _ -} 且不超过 64 位时原样使用；
     * 其余（含超长指纹、路径类攻击串）做 MD5 定长哈希，防特殊字符污染 key 结构。</p>
     */
    private String normalizeDeviceId(String deviceId) {
        if (StrUtil.isBlank(deviceId)) {
            return "default";
        }
        String trimmed = deviceId.trim();
        if (trimmed.matches("[A-Za-z0-9._-]{1,64}")) {
            return trimmed;
        }
        return SecureUtil.md5(trimmed);
    }

    @Override
    public Result sign() {
        //1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //2.获取日期
        LocalDateTime now = LocalDateTime.now();
        //3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4.获取今天是这个月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5.写入redis setbit key offset 1
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth-1,true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //2.获取日期
        LocalDateTime now = LocalDateTime.now();
        //3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4.获取今天是这个月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5.获取本月截止今天为止所有的签到记录，返回的是一个十进制的数字
        List<Long> result = stringRedisTemplate.opsForValue()
                .bitField(key, BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth))
                        .valueAt(0));
        if(result==null||result.isEmpty()){
            //没有任何签到结果
            return Result.ok(0);
        }
        Long num = result.get(0);
        if(num==0||num==null){
            return Result.ok(0);
        }
        //6.循坏遍历
        int count=0;
        while (true) {
            //让这个数字与1做与运算，得到数字的最后一个bit位，判断这个bit是否为0
            if((num&1)==0) {
                //如果为0，未签到
                break;
            }else {
                //如果不为0，已签到，计算器+1
                count++;
                //把数字右移一位，抛弃最后一个bit位，继续下一个bit位
                num>>>=1;
            }
        }
        return Result.ok(count);

    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX +RandomUtil.randomString(10));
        //保存
        save(user);
        return user;

    }
}
