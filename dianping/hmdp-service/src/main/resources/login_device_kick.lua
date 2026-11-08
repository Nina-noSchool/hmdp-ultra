-- 登录设备互踢：原子完成「踢同设备旧 token + 交换设备指针」
-- 调用方：UserServiceImpl.login()（Spring Data Redis execute，单次网络往返保证踢人与换锚不撕裂）
--
-- KEYS[1] = login:device:{userId}:{deviceId}   设备指针（value = 该设备当前生效的 token）
-- ARGV[1] = 本次登录的新 token
-- ARGV[2] = 指针 TTL（秒），与会话 Hash 的 TTL 保持一致
--
-- 返回值：被踢掉的旧 token（没有旧会话则返回空串），调用方据此记录互踢日志。
-- 注意：旧 token 的会话 key 前缀 'login:token:' 必须与 RedisConstants.LOGIN_USER_KEY 一致。
local old = redis.call('GET', KEYS[1])
local kicked = ''
if old and old ~= '' and old ~= ARGV[1] then
    redis.call('DEL', 'login:token:' .. old)
    kicked = old
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
return kicked
