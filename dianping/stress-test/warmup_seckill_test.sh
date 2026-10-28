#!/bin/bash
# ============================================================================
# 秒杀预热脚本：用独立预热券把 MQ 客户端/JIT/连接池焐热，再跑正式压测。
#
# 为什么需要（2026-10-27 实测）：
#   冷 JVM + 4000/3s 冲激下，老栈（JDK8 + rocketmq-spring 2.1.1）第一波
#   Tomcat 工作线程（800 个）集体阻塞在 syncSend 上直至超时，800 单回滚，
#   正式数据直接作废。顺序化的小批量预热能建立 producer 连接、触发 JIT、
#   焐热 broker 页缓存，把"第一波"伤害转移到正式压测之前。
#
# 防呆设计（提前踩坑）：
#   1) 预热券必须独立于压测券 —— 一人一单/去重/基线断言互不污染；
#   2) 预热前断言券行存在（防止忘了跑 seed_warmup_voucher.sql 时
#      300 单全部 OUT_OF_STOCK 空转）；
#   3) 预热后等 DB 消费收敛，防止预热消息和正式压测消息在 consumer 里混跑；
#   4) 清理预热失败落库的 tb_mq_event_record 残留（按 payload 匹配预热券），
#      防止 MqEventRetryJob 在正式压测进行中途回放它们制造 DB 噪声。
#
# 用法: MYSQL_PASS=<密码> APP_PORT=8081 ./warmup_seckill_test.sh [单数] [券ID]
# 默认: 300 单, 券 999
# ============================================================================
set -euo pipefail

WARMUP_COUNT=${1:-300}
WARMUP_VOUCHER_ID=${2:-999}
APP_PORT=${APP_PORT:-8081}
WARMUP_TIMEOUT=${WARMUP_TIMEOUT:-90}
MYSQL_HOST=${MYSQL_HOST:-127.0.0.1}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_PASS="${MYSQL_PASS:?MYSQL_PASS 未设置：请先 export MYSQL_PASS=<密码> 再运行}"
MYSQL_DB=${MYSQL_DB:-dingping}
MYSQL_CONTAINER=${MYSQL_CONTAINER:-hmdp-mysql}
REDIS_HOST=${REDIS_HOST:-127.0.0.1}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_PASSWORD=${REDIS_PASSWORD:-}

export MYSQL_PWD="${MYSQL_PASS}"
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
echo "  秒杀预热"
echo "  预热单数: ${WARMUP_COUNT}"
echo "  预热券:   ${WARMUP_VOUCHER_ID}"
echo "  应用端口: ${APP_PORT}"
echo "=========================================="

# ---- 1. 应用必须在线 ----
HEALTH=$(curl --max-time 3 -s "http://127.0.0.1:${APP_PORT}/actuator/health" || true)
echo "${HEALTH}" | grep -q '"status":"UP"' || fail "应用未就绪: ${HEALTH}"

# ---- 2. 券行必须存在（忘了跑 seed_warmup_voucher.sql 会 300 单空转） ----
ROWS=$(query "SELECT COUNT(*) FROM tb_seckill_voucher WHERE voucher_id = ${WARMUP_VOUCHER_ID}")
[ "${ROWS}" = "1" ] || fail "预热券 ${WARMUP_VOUCHER_ID} 不在 tb_seckill_voucher：先执行 seed_warmup_voucher.sql"

# ---- 3. 重置预热券的 Redis 状态 ----
"${REDIS_CMD[@]}" del "seckill:stock:${WARMUP_VOUCHER_ID}" \
  "seckill:order:${WARMUP_VOUCHER_ID}" "seckill:trace:log:${WARMUP_VOUCHER_ID}" >/dev/null
while IFS= read -r key; do
  [ -n "${key}" ] && "${REDIS_CMD[@]}" del "${key}" >/dev/null
done < <("${REDIS_CMD[@]}" --scan --pattern "seckill:req:${WARMUP_VOUCHER_ID}:*" || true)
"${REDIS_CMD[@]}" set "seckill:stock:${WARMUP_VOUCHER_ID}" "${WARMUP_COUNT}" >/dev/null

# ---- 4. 顺序预热（每个用户一单，天然避开限流/去重/一人一单） ----
echo "  发送 ${WARMUP_COUNT} 个预热请求..."
HTTP_200=0
HTTP_OTHER=0
i=1
while [ "${i}" -le "${WARMUP_COUNT}" ]; do
  CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 30 \
    -X POST -H "authorization: tokentest${i}" -H 'Content-Type: application/json' \
    "http://127.0.0.1:${APP_PORT}/voucher-order/seckill-plus/${WARMUP_VOUCHER_ID}" || echo 000)
  if [ "${CODE}" = "200" ]; then
    HTTP_200=$((HTTP_200 + 1))
  else
    HTTP_OTHER=$((HTTP_OTHER + 1))
  fi
  i=$((i + 1))
done
echo "  HTTP 200: ${HTTP_200}  其他: ${HTTP_OTHER}"

LUA_OK=$("${REDIS_CMD[@]}" scard "seckill:order:${WARMUP_VOUCHER_ID}")
MIN_OK=$((WARMUP_COUNT / 2))
[ "${LUA_OK}" -ge "${MIN_OK}" ] \
  || fail "预热 Lua 成功 ${LUA_OK}/${WARMUP_COUNT}，低于一半 —— MQ 链路可能整体不可用，先排查再压测"

# ---- 5. 等待预热订单落库（>= Lua 成功数即可：重放会多建 999 订单，无害） ----
echo "  等待预热订单落库（最多 ${WARMUP_TIMEOUT}s）..."
DEADLINE=$(( $(date +%s) + WARMUP_TIMEOUT ))
while true; do
  DB_999=$(query "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id = ${WARMUP_VOUCHER_ID}")
  [ "${DB_999}" -ge "${LUA_OK}" ] && { echo "  预热落库完成：DB=${DB_999} >= Lua=${LUA_OK}"; break; }
  [ "$(date +%s)" -ge "${DEADLINE}" ] \
    && fail "预热订单落库超时：DB=${DB_999} < Lua=${LUA_OK}，消费者可能卡住"
  sleep 2
done

# ---- 6. 清理预热失败的本地消息残留（防正式压测中途被 MqEventRetryJob 回放） ----
LEFTOVER=$(query "DELETE FROM tb_mq_event_record
  WHERE event_type = 'SECKILL_ORDER_CREATE' AND payload LIKE '%\"voucherId\":${WARMUP_VOUCHER_ID}%';
  SELECT ROW_COUNT();")
echo "  清理预热残留本地消息: ${LEFTOVER} 条"

echo ""
echo "WARMUP_OK count=${WARMUP_COUNT} voucher=${WARMUP_VOUCHER_ID} lua_ok=${LUA_OK} db=${DB_999}"
