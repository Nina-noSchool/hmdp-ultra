-- 布隆过滤器写入
-- KEYS[1] = BitMap key；ARGV[1..k] = 该值对应的 k 个位偏移
-- 语义：把 k 个位全部置 1（Lua 保证 k 次 SETBIT 原子完成）
for i = 1, #ARGV do
    redis.call('SETBIT', KEYS[1], ARGV[i], 1)
end
return 1
