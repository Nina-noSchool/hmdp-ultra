-- 预热专用券（幂等，可重复执行）
-- 用途：三列压测前，用独立券 999 把 MQ 客户端连接、JIT、连接池预热，
--       消除冷 JVM 第一波 syncSend 超时（老栈 4000/3s 下实测 800 次）。
-- 执行方式：mysql -uroot -p dingping < seed_warmup_voucher.sql
--
-- 为什么用独立券而不是券 13：
--   1) 一人一单：预热用户在 999 上下单不会占用 13 的一人一单资格；
--   2) 隔离：预热订单/日志/任务全部落在 voucher_id=999，基线断言只查 13，互不污染；
--   3) 预热的 Redis 库存由 prepare/warmup 脚本负责重置，这里只保证行存在。
--
-- stock 只是占位值；每轮 prepare 会把它重置为并发数。
INSERT INTO tb_seckill_voucher (voucher_id, stock, begin_time, end_time)
VALUES (999, 4000, '2020-01-01 00:00:00', '2030-01-01 00:00:00')
ON DUPLICATE KEY UPDATE stock = VALUES(stock);

-- 自检
SELECT voucher_id, stock FROM tb_seckill_voucher WHERE voucher_id = 999;
