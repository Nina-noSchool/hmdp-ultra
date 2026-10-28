#!/bin/bash
# ============================================================================
# 秒杀Plus压测准备脚本（安全版）
# 用法: MYSQL_PASS=<密码> ./prepare_seckill_test.sh [用户数] [券ID]
# 默认: 1000用户, 券ID=13
#
# 安全约束：
#   1) 默认要求应用已停止（运行中的 Job/消费者会与清库竞争，造成脏基线）
#   2) 默认先重置 RocketMQ 消费位点（跳过历史积压与 20 分钟延时消息）
#   3) 清库/设库存/生成 token 后逐项断言，任何一项不满足直接退出
#
# 可选环境变量：
#   ALLOW_RUNNING_APP=1   允许在应用运行时执行（仅调试用，不建议）
#   SKIP_BROKER_RESET=1   跳过 broker 位点重置（仅首次无积压时使用）
#   REDIS_HOST/REDIS_PORT/REDIS_PASSWORD   Redis 连接参数
# ============================================================================
set -euo pipefail

USER_COUNT=${1:-1000}
VOUCHER_ID=${2:-13}
MYSQL_HOST=${MYSQL_HOST:-127.0.0.1}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_PASS="${MYSQL_PASS:?MYSQL_PASS 未设置：请先 export MYSQL_PASS=<密码> 再运行}"
MYSQL_DB=${MYSQL_DB:-dingping}
MYSQL_CONTAINER=${MYSQL_CONTAINER:-hmdp-mysql}
REDIS_HOST=${REDIS_HOST:-127.0.0.1}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_PASSWORD=${REDIS_PASSWORD:-}
CSV_FILE=${CSV_FILE:-/tmp/seckill_tokens.csv}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ALLOW_RUNNING_APP=${ALLOW_RUNNING_APP:-0}

export MYSQL_PWD="${MYSQL_PASS}"
# VM 上的 /usr/local/bin/mysql 是 docker exec 包装脚本，不会把宿主环境变量带进容器；
# 显式 -e MYSQL_PWD 转发密码，既避免密码进 argv，也避免 MYSQL_PWD 丢失。
if command -v docker >/dev/null 2>&1 \
   && docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "${MYSQL_CONTAINER}"; then
  MYSQL_CMD=(docker exec -i -e MYSQL_PWD "${MYSQL_CONTAINER}" mysql \
    --default-character-set=utf8mb4 -h "${MYSQL_HOST}" -u "${MYSQL_USER}" -D "${MYSQL_DB}" -N -s)
else
  MYSQL_CMD=(mysql -h "${MYSQL_HOST}" -u "${MYSQL_USER}" -D "${MYSQL_DB}" -N -s)
fi
REDIS_CMD=(redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}")
if [ -n "${REDIS_PASSWORD}" ]; then
  REDIS_CMD+=(-a "${REDIS_PASSWORD}" --no-auth-warning)
fi

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

query() {
  "${MYSQL_CMD[@]}" -e "$1"
}

echo "=========================================="
echo "  秒杀Plus压测准备（安全版）"
echo "  用户数: ${USER_COUNT}"
echo "  券ID:   ${VOUCHER_ID}"
echo "=========================================="

# ---- 0. 应用必须停止 ----
APP_COUNT=$(ps -eo comm=,args= | awk '$1 == "java" && $0 ~ /hmdp.*\.jar/ {count++} END {print count+0}')
if [ "${APP_COUNT}" -gt 0 ]; then
  if [ "${ALLOW_RUNNING_APP}" != "1" ]; then
    echo "[FAIL] 检测到 ${APP_COUNT} 个应用实例仍在运行；运行中的 Job/消费者会与清库竞争。" >&2
    echo "       请先停止应用（kill \$(cat ~/hmdp-mq/app.pid) 等），再执行本脚本。" >&2
    echo "       仅调试时可设置 ALLOW_RUNNING_APP=1 强制继续。" >&2
    ps -eo pid=,comm=,args= | awk '$2 == "java" && $0 ~ /hmdp.*\.jar/ {print "         " $0}' >&2
    exit 1
  fi
  echo "[WARN] ALLOW_RUNNING_APP=1：应用仍在运行，脏基线风险由操作者承担"
fi

# ---- 1. 重置 broker 消费位点（跳过历史积压与延时消息） ----
if [ "${SKIP_BROKER_RESET:-0}" != "1" ]; then
  if [ ! -f "${SCRIPT_DIR}/reset_broker.sh" ]; then
    fail "缺少 ${SCRIPT_DIR}/reset_broker.sh；无法保证旧消息不重放。首次无积压可设 SKIP_BROKER_RESET=1"
  fi
  bash "${SCRIPT_DIR}/reset_broker.sh"
else
  echo "[WARN] SKIP_BROKER_RESET=1：跳过 broker 位点重置，旧消息可能重放"
fi

# ---- 2. 基础连通性与券存在性 ----
echo ""
echo "[1/8] 检查 MySQL / Redis / 券..."
query "SELECT 1" >/dev/null
PONG=$("${REDIS_CMD[@]}" ping)
[ "${PONG}" = "PONG" ] || fail "Redis ping 失败: ${PONG}"
VOUCHER_ROWS=$(query "SELECT COUNT(*) FROM tb_seckill_voucher WHERE voucher_id = ${VOUCHER_ID}")
[ "${VOUCHER_ROWS}" = "1" ] || fail "券 ${VOUCHER_ID} 不存在（tb_seckill_voucher 匹配行数=${VOUCHER_ROWS}）"
echo "  连通性 OK，券存在"

# ---- 3. 清理 DB 测试数据 ----
echo ""
echo "[2/8] 清理DB旧数据（券ID=${VOUCHER_ID}）..."
OLD_ORDERS=$(query "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id = ${VOUCHER_ID}")
OLD_LOGS=$(query "SELECT COUNT(*) FROM tb_voucher_reconcile_log WHERE voucher_id = ${VOUCHER_ID}")
OLD_VERIFY=$(query "SELECT COUNT(*) FROM tb_order_create_verify_task WHERE voucher_id = ${VOUCHER_ID}")
OLD_RECOVERY=$(query "SELECT COUNT(*) FROM tb_order_create_recovery_task WHERE voucher_id = ${VOUCHER_ID}")
OLD_RECONCILE=$(query "SELECT COUNT(*) FROM tb_seckill_reconcile_task WHERE voucher_id = ${VOUCHER_ID}")
echo "  旧订单: ${OLD_ORDERS}  对账日志: ${OLD_LOGS}  核验任务: ${OLD_VERIFY}  恢复任务: ${OLD_RECOVERY}  对账任务: ${OLD_RECONCILE}"

query "DELETE FROM tb_voucher_order WHERE voucher_id = ${VOUCHER_ID};
       DELETE FROM tb_voucher_reconcile_log WHERE voucher_id = ${VOUCHER_ID};
       DELETE FROM tb_order_create_verify_task WHERE voucher_id = ${VOUCHER_ID};
       DELETE FROM tb_order_create_recovery_task WHERE voucher_id = ${VOUCHER_ID};
       DELETE FROM tb_seckill_reconcile_task WHERE voucher_id = ${VOUCHER_ID};
       DELETE FROM tb_mq_event_record;"

# ---- 3b. 清理额外券（预热专用券等）—— 防止跨轮/跨列数据污染 ----
for extra in ${EXTRA_VOUCHER_IDS:-}; do
  if [ "${extra}" = "${VOUCHER_ID}" ]; then
    continue
  fi
  echo "  额外清理预热券 ${extra}..."
  EXTRA_ROWS=$(query "SELECT COUNT(*) FROM tb_seckill_voucher WHERE voucher_id = ${extra}")
  [ "${EXTRA_ROWS}" = "1" ] || fail "额外券 ${extra} 不在 tb_seckill_voucher：先执行 seed_warmup_voucher.sql"
  query "DELETE FROM tb_voucher_order WHERE voucher_id = ${extra};
         DELETE FROM tb_voucher_reconcile_log WHERE voucher_id = ${extra};
         DELETE FROM tb_order_create_verify_task WHERE voucher_id = ${extra};
         DELETE FROM tb_order_create_recovery_task WHERE voucher_id = ${extra};
         DELETE FROM tb_seckill_reconcile_task WHERE voucher_id = ${extra};"
  query "UPDATE tb_seckill_voucher SET stock = ${USER_COUNT} WHERE voucher_id = ${extra};
         SELECT ROW_COUNT();" >/dev/null
  "${REDIS_CMD[@]}" del "seckill:stock:${extra}" "seckill:order:${extra}" "seckill:trace:log:${extra}" >/dev/null
  while IFS= read -r key; do
    [ -n "${key}" ] && "${REDIS_CMD[@]}" del "${key}" >/dev/null
  done < <("${REDIS_CMD[@]}" --scan --pattern "seckill:req:${extra}:*" || true)
done

# ---- 4. 同步 DB 库存 ----
echo ""
echo "[3/8] 同步数据库库存..."
UPDATED=$(query "UPDATE tb_seckill_voucher SET stock = ${USER_COUNT} WHERE voucher_id = ${VOUCHER_ID}; SELECT ROW_COUNT();")
[ "${UPDATED}" = "1" ] || fail "DB 库存更新失败（ROW_COUNT=${UPDATED}，券 ${VOUCHER_ID} 不存在？）"
echo "  DB stock set to ${USER_COUNT}"

# ---- 5. 清理 Redis 旧数据 ----
echo "[4/8] 清理Redis旧数据（券ID=${VOUCHER_ID}）..."
"${REDIS_CMD[@]}" del "seckill:stock:${VOUCHER_ID}" "seckill:order:${VOUCHER_ID}" "seckill:trace:log:${VOUCHER_ID}" >/dev/null
while IFS= read -r key; do
  [ -n "${key}" ] && "${REDIS_CMD[@]}" del "${key}" >/dev/null
done < <("${REDIS_CMD[@]}" --scan --pattern "seckill:req:${VOUCHER_ID}:*" || true)
while IFS= read -r key; do
  [ -n "${key}" ] && "${REDIS_CMD[@]}" del "${key}" >/dev/null
done < <("${REDIS_CMD[@]}" --scan --pattern "rate:sw:seckill:*" || true)
echo "  旧数据已清理"

# ---- 6. 清理旧 token 并生成新 token ----
echo "[5/8] 清理旧测试token并生成 ${USER_COUNT} 个新token..."
while IFS= read -r key; do
  [ -n "${key}" ] && "${REDIS_CMD[@]}" del "${key}" >/dev/null
done < <("${REDIS_CMD[@]}" --scan --pattern 'login:token:tokentest*' || true)

GEN_COUNT=$("${REDIS_CMD[@]}" --eval /dev/stdin <<LUA
  local count = 0
  for i = 1, ${USER_COUNT} do
    local token = "tokentest" .. i
    local key = "login:token:" .. token
    redis.call('hset', key, 'id', i, 'nickName', 'user' .. i, 'icon', '')
    redis.call('expire', key, 36000)
    count = count + 1
  end
  return count
LUA
)
[ "${GEN_COUNT}" = "${USER_COUNT}" ] || fail "token 生成数量不符：期望 ${USER_COUNT}，实际 ${GEN_COUNT}"

# ---- 7. 设置 Redis 库存 ----
echo "[6/8] 设置Redis库存 = ${USER_COUNT}..."
"${REDIS_CMD[@]}" set "seckill:stock:${VOUCHER_ID}" "${USER_COUNT}" >/dev/null

# ---- 8. 生成 CSV ----
echo "[7/8] 生成CSV文件: ${CSV_FILE}..."
: > "${CSV_FILE}"
for i in $(seq 1 "${USER_COUNT}"); do
  echo "tokentest$i" >> "${CSV_FILE}"
done

# ---- 9. 断言基线 ----
echo "[8/8] 断言基线..."
REDIS_STOCK=$("${REDIS_CMD[@]}" get "seckill:stock:${VOUCHER_ID}")
REDIS_ORDERS=$("${REDIS_CMD[@]}" scard "seckill:order:${VOUCHER_ID}")
TOKEN_COUNT=$("${REDIS_CMD[@]}" --scan --pattern 'login:token:tokentest*' | wc -l | tr -d ' ')
CSV_LINES=$(wc -l < "${CSV_FILE}" | tr -d ' ')
DB_STOCK=$(query "SELECT stock FROM tb_seckill_voucher WHERE voucher_id = ${VOUCHER_ID}")
DB_ORDERS=$(query "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id = ${VOUCHER_ID}")
DB_LOGS=$(query "SELECT COUNT(*) FROM tb_voucher_reconcile_log WHERE voucher_id = ${VOUCHER_ID}")
DB_VERIFY=$(query "SELECT COUNT(*) FROM tb_order_create_verify_task WHERE voucher_id = ${VOUCHER_ID}")
DB_RECOVERY=$(query "SELECT COUNT(*) FROM tb_order_create_recovery_task WHERE voucher_id = ${VOUCHER_ID}")
DB_RECONCILE=$(query "SELECT COUNT(*) FROM tb_seckill_reconcile_task WHERE voucher_id = ${VOUCHER_ID}")
DB_MQ=$(query "SELECT COUNT(*) FROM tb_mq_event_record")

echo "  Redis stock=${REDIS_STOCK} orders=${REDIS_ORDERS} tokens=${TOKEN_COUNT} csv=${CSV_LINES}"
echo "  DB stock=${DB_STOCK} orders=${DB_ORDERS} logs=${DB_LOGS} verify=${DB_VERIFY} recovery=${DB_RECOVERY} reconcile=${DB_RECONCILE} mq=${DB_MQ}"

[ "${REDIS_STOCK}" = "${USER_COUNT}" ] || fail "Redis stock 期望 ${USER_COUNT}，实际 ${REDIS_STOCK}"
[ "${REDIS_ORDERS}" = "0" ] || fail "Redis orders 期望 0，实际 ${REDIS_ORDERS}"
[ "${TOKEN_COUNT}" = "${USER_COUNT}" ] || fail "token 数期望 ${USER_COUNT}，实际 ${TOKEN_COUNT}"
[ "${CSV_LINES}" = "${USER_COUNT}" ] || fail "CSV 行数期望 ${USER_COUNT}，实际 ${CSV_LINES}"
[ "${DB_STOCK}" = "${USER_COUNT}" ] || fail "DB stock 期望 ${USER_COUNT}，实际 ${DB_STOCK}"
[ "${DB_ORDERS}" = "0" ] || fail "DB orders 期望 0，实际 ${DB_ORDERS}（仍有消息在重放，检查 broker/本地消息表/应用是否已停）"
[ "${DB_LOGS}" = "0" ] || fail "DB reconcile logs 期望 0，实际 ${DB_LOGS}"
[ "${DB_VERIFY}" = "0" ] || fail "DB verify tasks 期望 0，实际 ${DB_VERIFY}"
[ "${DB_RECOVERY}" = "0" ] || fail "DB recovery tasks 期望 0，实际 ${DB_RECOVERY}"
[ "${DB_RECONCILE}" = "0" ] || fail "DB reconcile tasks 期望 0，实际 ${DB_RECONCILE}"
[ "${DB_MQ}" = "0" ] || fail "DB mq_event_record 期望 0，实际 ${DB_MQ}"

echo ""
echo "PREPARE_OK user_count=${USER_COUNT} voucher_id=${VOUCHER_ID} db_stock=${DB_STOCK} redis_stock=${REDIS_STOCK}"
echo "下一步：启动应用（benchmark profile）后执行 run_seckill_test.sh"
