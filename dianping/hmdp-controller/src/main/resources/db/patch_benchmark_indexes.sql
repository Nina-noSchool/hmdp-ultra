-- 压测相关索引补丁（幂等，可重复执行）
-- 执行方式：mysql -uroot -p dingping < patch_benchmark_indexes.sql
--
-- 加索引的依据（不是“感觉慢”）：
--   1) SeckillReconcileScanService.scanOrders():
--        WHERE create_time <= now - 3min LIMIT 100
--      tb_voucher_order 只有主键和 uk_user_voucher，create_time 无索引 → 全表扫。
--   2) SeckillReconcileScanService.scanDeductLogs():
--        WHERE log_type = 'DEDUCT' AND create_time <= now - 3min LIMIT 100
--      已有 idx_voucher_log_type(voucher_id, log_type)，前导列不是 log_type，用不上。
--   3) 压测准备/结果核对按 voucher_id 统计订单、核验任务、恢复任务；
--      这三张表都只有 order_id / trace_id / status 索引，voucher_id 无索引。
--
-- 注意：MySQL 8 不支持 CREATE INDEX IF NOT EXISTS，这里用存储过程查
-- information_schema.statistics，保证脚本可以重复执行而不报 Duplicate key name。

DROP PROCEDURE IF EXISTS add_index_if_missing;

DELIMITER $$
CREATE PROCEDURE add_index_if_missing(
    IN p_table VARCHAR(64),
    IN p_index VARCHAR(64),
    IN p_columns VARCHAR(255)
)
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = p_table
          AND index_name = p_index
    ) THEN
        SET @ddl = CONCAT('ALTER TABLE `', p_table, '` ADD INDEX `', p_index, '` (', p_columns, ')');
        PREPARE stmt FROM @ddl;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END$$
DELIMITER ;

CALL add_index_if_missing('tb_voucher_order', 'idx_voucher_id', '`voucher_id`');
CALL add_index_if_missing('tb_voucher_order', 'idx_create_time', '`create_time`');
CALL add_index_if_missing('tb_order_create_verify_task', 'idx_voucher_id', '`voucher_id`');
CALL add_index_if_missing('tb_order_create_recovery_task', 'idx_voucher_id', '`voucher_id`');
CALL add_index_if_missing('tb_voucher_reconcile_log', 'idx_log_type_create_time', '`log_type`,`create_time`');

DROP PROCEDURE IF EXISTS add_index_if_missing;

-- 自检：应当能看到上面 5 个索引
SELECT table_name, index_name, GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns
FROM information_schema.statistics
WHERE table_schema = DATABASE()
  AND (
        (table_name = 'tb_voucher_order' AND index_name IN ('idx_voucher_id', 'idx_create_time'))
     OR (table_name = 'tb_order_create_verify_task' AND index_name = 'idx_voucher_id')
     OR (table_name = 'tb_order_create_recovery_task' AND index_name = 'idx_voucher_id')
     OR (table_name = 'tb_voucher_reconcile_log' AND index_name = 'idx_log_type_create_time')
  )
GROUP BY table_name, index_name
ORDER BY table_name, index_name;
