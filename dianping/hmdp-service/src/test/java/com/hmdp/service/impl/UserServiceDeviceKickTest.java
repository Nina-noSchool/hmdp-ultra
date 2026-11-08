package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.User;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 设备互踢单元测试：验证登录时设备绑定、指针交换与登出只删会话的调用契约。
 * 互踢的原子性由 login_device_kick.lua 在 Redis 侧保证（联调/压测覆盖），
 * 这里聚焦服务层把"正确的 key、正确的参数"交给正确的脚本。
 * 注：Mock 交互统一用 thenAnswer 记录 + 数据断言，不使用 verify+匹配器形态。
 */
class UserServiceDeviceKickTest {

    private static final String PHONE = "13800138000";
    private static final String CODE = "123456";
    private static final String BAD_DEVICE_ID = "bad device id!";

    private StringRedisTemplate template;
    @SuppressWarnings("unchecked")
    private final HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);
    private QueryChainWrapper<User> queryChain;
    private UserServiceImpl service;

    // 记录式捕获：脚本调用（指针 key / 新 token / TTL）与 Redis 删除、续期、会话写入
    private final List<String> pointerKeys = new ArrayList<>();
    private final List<String> scriptNewTokens = new ArrayList<>();
    private final List<String> scriptTtls = new ArrayList<>();
    private final AtomicReference<String> scriptReply = new AtomicReference<>("");
    private final List<String> deletedKeys = new ArrayList<>();
    private final List<String> expiredKeys = new ArrayList<>();
    private final List<Map<String, Object>> savedHashes = new ArrayList<>();

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        template = mock(StringRedisTemplate.class);
        when(template.opsForHash()).thenReturn(hashOps);
        @SuppressWarnings("unchecked")
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(template.opsForValue()).thenReturn(valueOps);
        when(valueOps.get(RedisConstants.LOGIN_CODE_KEY + PHONE)).thenReturn(CODE);

        // query() 来自 MyBatis-Plus IService 的 default 方法，匿名子类覆写掉数据库访问；
        // 链式 eq/one 显式打桩（RETURNS_SELF 对泛型型变返回值 Children 解析不出会返回 null）
        queryChain = mock(QueryChainWrapper.class);
        when(queryChain.eq(anyString(), any())).thenReturn(queryChain);
        User user = new User();
        user.setId(9527L);
        user.setPhone(PHONE);
        user.setNickName("tester");
        when(queryChain.one()).thenReturn(user);

        service = new UserServiceImpl() {
            @Override
            public QueryChainWrapper<User> query() {
                return queryChain;
            }
        };
        setField(service, "stringRedisTemplate", template);

        when(template.execute(any(RedisScript.class), anyList(), any(Object[].class))).thenAnswer(inv -> {
            Object[] raw = inv.getArguments();
            pointerKeys.add((String) ((List<?>) raw[1]).get(0));
            // varargs 槽位兼容读取：不同 Mockito 版本可能整体成数组或已展开
            List<Object> scriptArgs = new ArrayList<>();
            for (int i = 2; i < raw.length; i++) {
                if (raw[i] instanceof Object[] values) {
                    scriptArgs.addAll(List.of(values));
                } else {
                    scriptArgs.add(raw[i]);
                }
            }
            scriptNewTokens.add((String) scriptArgs.get(0));
            scriptTtls.add((String) scriptArgs.get(1));
            return scriptReply.get();
        });
        when(template.delete(anyString())).thenAnswer(inv -> {
            deletedKeys.add((String) inv.getArgument(0));
            return Boolean.TRUE;
        });
        when(template.expire(anyString(), anyLong(), any(TimeUnit.class))).thenAnswer(inv -> {
            expiredKeys.add((String) inv.getArgument(0));
            return Boolean.TRUE;
        });
        doAnswer(inv -> {
            savedHashes.add((Map<String, Object>) inv.getArgument(1));
            return null;
        }).when(hashOps).putAll(anyString(), anyMap());
    }

    @Test
    void login_withoutDeviceHeader_bindsDefaultDeviceAndSwapsPointer() {
        scriptReply.set("");

        Result result = service.login(form(), null, null);

        assertTrue(result.getSuccess());
        String token = (String) result.getData();
        assertNotNull(token);
        // 会话 Hash 写入且带 deviceId 字段（拦截器据此同步续期设备指针）
        assertEquals(1, savedHashes.size());
        assertEquals("default", savedHashes.get(0).get("deviceId"));
        // 会话 TTL 30 分钟
        assertTrue(expiredKeys.contains(RedisConstants.LOGIN_USER_KEY + token));
        // 设备指针交换：key = login:device:{userId}:{deviceId}，ARGV = 新 token + 1800 秒
        assertEquals(1, pointerKeys.size());
        assertEquals(RedisConstants.LOGIN_DEVICE_KEY + "9527:default", pointerKeys.get(0));
        assertEquals(token, scriptNewTokens.get(0));
        assertEquals("1800", scriptTtls.get(0));
    }

    @Test
    void login_withIllegalDeviceId_hashesDeviceIdIntoPointerKey() {
        scriptReply.set("");

        service.login(form(), null, BAD_DEVICE_ID);

        assertEquals(1, pointerKeys.size());
        String pointerKey = pointerKeys.get(0);
        String prefix = RedisConstants.LOGIN_DEVICE_KEY + "9527:";
        assertTrue(pointerKey.startsWith(prefix));
        // 非法字符被 MD5 定长哈希，不污染 Redis key 结构
        assertTrue(pointerKey.substring(prefix.length()).matches("[0-9a-f]{32}"),
                "非法设备标识应被定长哈希，实际 key=" + pointerKey);
    }

    @Test
    void login_sameDeviceReLogin_handsOldTokenKickToScript() {
        // Lua 返回被踢旧 token —— 服务层只负责接线，原子踢人发生在 Redis 侧
        scriptReply.set("old-token-value");

        Result result = service.login(form(), null, "web-pc");

        assertTrue(result.getSuccess());
        assertEquals(1, pointerKeys.size());
        assertEquals(RedisConstants.LOGIN_DEVICE_KEY + "9527:web-pc", pointerKeys.get(0));
        assertEquals(result.getData(), scriptNewTokens.get(0));
    }

    @Test
    void logout_deletesSessionHashOnly_keepsDevicePointer() {
        service.logout("t-abc");

        assertTrue(deletedKeys.contains(RedisConstants.LOGIN_USER_KEY + "t-abc"));
        assertTrue(deletedKeys.stream().noneMatch(k -> k.startsWith(RedisConstants.LOGIN_DEVICE_KEY)),
                "登出不应删除设备指针");
    }

    private LoginFormDTO form() {
        return LoginFormDTO.builder().phone(PHONE).code(CODE).build();
    }

    private static void setField(Object target, String name, Object value) {
        try {
            Field field = target.getClass().getSuperclass().getDeclaredField(name);
            field.setAccessible(true);
            field.set(target, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("字段注入失败: " + name, e);
        }
    }
}
