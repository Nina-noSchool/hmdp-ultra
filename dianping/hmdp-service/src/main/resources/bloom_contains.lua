-- 布隆过滤器包含性检查
-- KEYS[1] = BitMap key；ARGV[1..k] = 该值对应的 k 个位偏移
-- 语义：任一位为 0 ⇒ 一定不存在（返回 0）；全部为 1 ⇒ 可能存在（返回 1，误判率 = fpp）
for i = 1, #ARGV do
    if redis.call('GETBIT', KEYS[1], ARGV[i]) == 0 then
        return 0
    end
end
return 1
