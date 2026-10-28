#!/bin/bash
# ============================================================================
# 压测前 broker 位点重置：把秒杀两个消费组的主 topic 与 %RETRY% 重试 topic
# 都重置到最新，跳过历史积压、失败重试消息与延时消息
# 用法: ./reset_broker.sh
# 可覆盖环境变量：
#   MQ_BROKER_CONTAINER=hmdp-mq-broker
#   MQADMIN_IN_CONTAINER=/home/rocketmq/rocketmq-4.9.7/bin/mqadmin
#   MQ_NAMESRV=rocketmq-namesrv:9876   （docker exec 模式）
#   SECKILL_TOPIC=seckill-plus-order-topic
#   SECKILL_CONSUMER_GROUPS="seckill-plus-order-consumer-group seckill-plus-timeout-consumer-group"
# ============================================================================
set -euo pipefail

if [ "${SKIP_BROKER_RESET:-0}" = "1" ]; then
  echo "[WARN] SKIP_BROKER_RESET=1：跳过 broker 位点重置"
  exit 0
fi

TOPIC=${SECKILL_TOPIC:-seckill-plus-order-topic}
GROUPS_STR=${SECKILL_CONSUMER_GROUPS:-"seckill-plus-order-consumer-group seckill-plus-timeout-consumer-group"}
BROKER_CONTAINER=${MQ_BROKER_CONTAINER:-hmdp-mq-broker}
MQADMIN_IN_CONTAINER=${MQADMIN_IN_CONTAINER:-/home/rocketmq/rocketmq-4.9.7/bin/mqadmin}

if command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' | grep -qx "${BROKER_CONTAINER}"; then
  NAMESRV=${MQ_NAMESRV:-rocketmq-namesrv:9876}
  MQADMIN=(docker exec "${BROKER_CONTAINER}" "${MQADMIN_IN_CONTAINER}")
elif command -v mqadmin >/dev/null 2>&1; then
  NAMESRV=${MQ_NAMESRV:-127.0.0.1:9876}
  MQADMIN=(mqadmin)
else
  echo "[FAIL] 找不到 mqadmin（容器 ${BROKER_CONTAINER} 未运行，宿主机也没有 mqadmin）" >&2
  exit 1
fi

echo "=========================================="
echo "  RocketMQ 位点重置（跳过历史积压/延时消息）"
echo "  topic:   ${TOPIC}"
echo "  groups:  ${GROUPS_STR}"
echo "  namesrv: ${NAMESRV}"
echo "=========================================="

# RocketMQ 4.9.x：-s 使用 "yyyy-MM-dd#HH:mm:ss:SSS" 时间；-f 允许向前重置到当前时间
RESET_TS=$(date '+%Y-%m-%d#%H:%M:%S:000')

reset_topic_offset() {
  local topic=$1
  local group=$2
  echo "  reset group=${group} topic=${topic} -> ${RESET_TS} ..."
  "${MQADMIN[@]}" resetOffsetByTime -n "${NAMESRV}" -g "${group}" -t "${topic}" -s "${RESET_TS}" -f true
}

# 重试消息在 %RETRY%<group> 里，主 topic 的位点重置不会清掉它们；
# 不处理的话，上一轮消费失败的消息会在应用启动后重放成新订单。
TOPIC_LIST=$("${MQADMIN[@]}" topicList -n "${NAMESRV}" 2>/dev/null || true)
for group in ${GROUPS_STR}; do
  reset_topic_offset "${TOPIC}" "${group}"
  RETRY_TOPIC="%RETRY%${group}"
  if printf '%s\n' "${TOPIC_LIST}" | grep -qx "${RETRY_TOPIC}"; then
    reset_topic_offset "${RETRY_TOPIC}" "${group}"
  else
    echo "  retry topic ${RETRY_TOPIC} 不存在，跳过"
  fi
done

echo "BROKER_RESET_OK"
