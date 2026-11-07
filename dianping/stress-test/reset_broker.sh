#!/bin/bash
# ============================================================================
# 压测前 broker 位点守卫：保证两个消费组在启动应用时没有可重放的存量消息
#
# 策略（v2，按"是否真有积压"决策，而不是无条件重置）：
#   1) 先用 consumerProgress 检查积压；若全部追平（diff=0）直接跳过——
#      无条件重置反而有害：RocketMQ 4.9 的 getOffsetInQueueByTime 对超出
#      末条消息存储时间的请求只把位点放到 maxOffset-1，每个队列留下 1 条
#      "尾部消息"待重放；用 -s now 则会因宿主机与 broker 的时钟偏差落在
#      尾部消息之前（曾造成基线脏 6 笔）。
#   2) 真有积压时才重置：用远未来时间戳（+1 天）跳过全部存量消息；
#      重置后每个队列仍可能残留 1 条末尾消息，需启动应用排空（drain）、
#      停止后再跑一轮 prepare（SKIP_BROKER_RESET=1），然后才能正式压测。
#
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
echo "  RocketMQ 位点守卫（无积压则跳过重置）"
echo "  topic:   ${TOPIC}"
echo "  groups:  ${GROUPS_STR}"
echo "  namesrv: ${NAMESRV}"
echo "=========================================="

# 统计一个消费组在主 topic 与 %RETRY% topic 上的总积压（brokerOffset-consumerOffset）
lag_of_group() {
  local group=$1
  local out lag=0
  out=$("${MQADMIN[@]}" consumerProgress -n "${NAMESRV}" -g "${group}" 2>/dev/null || true)
  local tname broker qid boff coff rest
  while read -r tname broker qid boff coff rest; do
    case "${tname}" in
      "${TOPIC}"|"%RETRY%${group}") ;;
      *) continue ;;
    esac
    if [[ "${boff}" =~ ^[0-9]+$ && "${coff}" =~ ^[0-9]+$ ]]; then
      lag=$(( lag + boff - coff ))
    fi
  done <<< "${out}"
  echo "${lag}"
}

TOTAL_LAG=0
for group in ${GROUPS_STR}; do
  LAG=$(lag_of_group "${group}")
  echo "  group=${group} backlog=${LAG}"
  TOTAL_LAG=$(( TOTAL_LAG + LAG ))
done

if [ "${TOTAL_LAG}" -le 0 ]; then
  echo "NO_BACKLOG_SKIP_RESET 消费组已追平，跳过重置（重置本身会留下尾部消息）"
  exit 0
fi

echo "[WARN] 检测到积压 ${TOTAL_LAG} 条：执行远未来时间戳重置"
echo "[WARN] 重置后每个队列可能残留 1 条末尾消息——请先启动应用排空（drain），"
echo "       停止应用后再跑一轮 prepare（SKIP_BROKER_RESET=1），然后再压测。"

# RocketMQ 4.9.x：-s 使用 "yyyy-MM-dd#HH:mm:ss:SSS" 时间；-f 允许向前重置
RESET_TS=$(date -d '+1 day' '+%Y-%m-%d#%H:%M:%S:000')

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

echo "BROKER_RESET_OK（积压已跳过；残留尾部消息需 drain 排空）"
