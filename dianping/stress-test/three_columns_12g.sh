#!/bin/bash
# ============================================================================
# 三列 R1 对照（12G VM）：JDK8 / JDK21 平台线程 / JDK21 虚拟线程
#
# 安全契约（第一类修复 + 质量增强）：
#   1) prepare 只在应用停止后执行，杜绝 Job/消费者与清库竞争；
#   2) 每轮开始前重置 broker 位点，并先启停一次应用消费掉每个队列的尾部旧消息，
#      再用 SKIP_BROKER_RESET=1 prepare，避免最后一条旧消息在正式压测时落库；
#   3) 每轮正式压测前用独立预热券（默认 999）做 seckill 预热，把 MQ 客户端/JIT/
#      连接池焐热 —— 冷 JVM 第一波 syncSend 超时（老栈实测 800 次）不能再毁掉数据；
#   4) 每列跑 REPEATS 轮，逐轮完整循环（含 preclean/reset/drain），
#      防止上一轮失败的本地消息表残留污染下一轮；
#   5) 三列统一 Hikari(20)、com.hmdp=INFO、send-message-timeout=10000；
#   6) jar 只按路径读取 + sha256 记录，不再 cp 覆盖 canonical jar；
#   7) 单实例/故障开关/基线断言由 run_seckill_test.sh 二次把关；
#   8) 结束时输出结果表（吞吐/一致性/MQ超时），任一轮一致性未过则整体非零退出。
#
# 用法（在 VM 上）：
#   bash ~/hmdp-mq/stress-test/three_columns_12g.sh
#
# 可覆盖：
#   THREADS=4000 RAMPUP=3 VOUCHER_ID=13 DRAIN_TIMEOUT=120 REPEATS=2
#   WARMUP_COUNT=300 WARMUP_VOUCHER_ID=999
#   JDK8_JAR=... JDK21_JAR=... JAVA8_BIN=... JAVA21_BIN=...
#   APP_HOME=~/hmdp-mq ENV_FILE=~/hmdp-infra/.env
# ============================================================================
set -euo pipefail

APP_HOME=${APP_HOME:-$HOME/hmdp-mq}
STRESS_DIR="${APP_HOME}/stress-test"
ENV_FILE=${ENV_FILE:-$HOME/hmdp-infra/.env}
THREADS=${THREADS:-4000}
RAMPUP=${RAMPUP:-3}
VOUCHER_ID=${VOUCHER_ID:-13}
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-120}
DRAIN_BROKER_SECONDS=${DRAIN_BROKER_SECONDS:-15}
REPEATS=${REPEATS:-2}
WARMUP_COUNT=${WARMUP_COUNT:-300}
WARMUP_VOUCHER_ID=${WARMUP_VOUCHER_ID:-999}
# 持续并发模式：>0 时每个 JMeter 线程循环 SUSTAINED_LOOPS 次
# （JMeter 线程数 = THREADS/SUSTAINED_LOOPS，总请求仍为 THREADS）。
# 动机：4000 线程 ramp-up 模式下 JMeter 建线程速率是假瓶颈，持续并发才能测出应用真实吞吐。
SUSTAINED_LOOPS=${SUSTAINED_LOOPS:-0}
# 选择跑哪些列：COLUMNS="col2 col3"（默认三列全跑）
COLUMNS=${COLUMNS:-"col1 col2 col3"}

JDK8_JAR=${JDK8_JAR:-${APP_HOME}/hmdp-start.pre-rerun-20261024.jar}
JDK21_JAR=${JDK21_JAR:-${APP_HOME}/hmdp-jdk21-main-saved.jar}
JAVA8_BIN=${JAVA8_BIN:-}
JAVA21_BIN=${JAVA21_BIN:-}

COL1_PORT=${COL1_PORT:-8081}
COL2_PORT=${COL2_PORT:-8082}
COL3_PORT=${COL3_PORT:-8082}

# 强制打开安全约束：即使外部环境里设过，也不允许在应用运行时清库
export ALLOW_RUNNING_APP=0
export SKIP_BROKER_RESET=0
if [ "${WARMUP_COUNT}" -gt 0 ]; then
  export EXTRA_VOUCHER_IDS="${WARMUP_VOUCHER_ID}"
else
  export EXTRA_VOUCHER_IDS=""
fi

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

list_app_pids() {
  # 只认 comm=java 且参数里带 hmdp*.jar 的进程：
  # col1 是 hmdp-start*.jar，col2/3 是 hmdp-jdk21-main-saved.jar，不能只匹配 hmdp-start。
  ps -eo pid=,comm=,args= | awk '$2 == "java" && $0 ~ /hmdp.*\.jar/ {print $1}'
}

stop_all_apps() {
  local pids
  pids=$(list_app_pids)
  if [ -z "${pids}" ]; then
    return 0
  fi
  echo "  停止应用进程: $(echo "${pids}" | tr '\n' ' ')"
  # shellcheck disable=SC2086
  kill ${pids} 2>/dev/null || true
  local i
  for i in $(seq 1 30); do
    pids=$(list_app_pids)
    if [ -z "${pids}" ]; then
      break
    fi
    sleep 1
  done
  pids=$(list_app_pids)
  if [ -n "${pids}" ]; then
    echo "  优雅停止超时，强制 kill -9"
    # shellcheck disable=SC2086
    kill -9 ${pids} 2>/dev/null || true
  fi
}

cleanup() {
  stop_all_apps
}
trap cleanup EXIT
trap 'exit 130' INT TERM

find_java() {
  local candidate
  for candidate in "$@"; do
    if [ -x "${candidate}" ]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  return 1
}

sha256_of() {
  sha256sum "$1" | awk '{print $1}'
}

wait_health() {
  local port=$1 pid=$2 log=$3 i
  for i in $(seq 1 120); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      echo "[FAIL] 应用进程已退出，日志末尾:" >&2
      tail -100 "${log}" >&2 || true
      return 1
    fi
    if curl --max-time 2 -fsS "http://127.0.0.1:${port}/actuator/health" 2>/dev/null \
        | grep -q '"status":"UP"'; then
      echo "  health UP after $((i * 2))s (port ${port})"
      return 0
    fi
    sleep 2
  done
  echo "[FAIL] 启动超时（240s），日志末尾:" >&2
  tail -100 "${log}" >&2 || true
  return 1
}

start_app() {
  local label=$1 jar=$2 java_bin=$3 port=$4
  shift 4
  local log="${APP_HOME}/app-${label}.log"
  local pidfile="${APP_HOME}/app-${label}.pid"

  # 防止上一个实例没死干净时，新进程绑定失败而 health 检查读到旧实例
  if curl --max-time 1 -s "http://127.0.0.1:${port}/actuator/health" 2>/dev/null \
      | grep -q '"status":"UP"'; then
    fail "端口 ${port} 已有健康应用，拒绝启动第二个实例"
  fi

  : > "${log}"
  nohup "${java_bin}" -Xms512m -Xmx1536m -XX:MaxMetaspaceSize=256m -jar "${jar}" \
    --server.port="${port}" \
    --spring.profiles.active=benchmark \
    --spring.datasource.hikari.maximum-pool-size=20 \
    --spring.datasource.hikari.minimum-idle=5 \
    --rocketmq.producer.send-message-timeout=10000 \
    --logging.level.com.hmdp=info \
    "$@" >> "${log}" 2>&1 < /dev/null &
  local pid=$!
  echo "${pid}" > "${pidfile}"
  echo "  start pid=${pid} port=${port} log=${log}"
  wait_health "${port}" "${pid}" "${log}"
}

RESULTS_FILE="${STRESS_DIR}/three_columns_results.tsv"
: > "${RESULTS_FILE}"

run_once() {
  local label=$1 jar=$2 java_bin=$3 port=$4 note=$5
  shift 5

  stop_all_apps

  echo "-- [1/7] preclean（应用已停止，SKIP_BROKER_RESET=1） --"
  SKIP_BROKER_RESET=1 bash "${STRESS_DIR}/prepare_seckill_test.sh" "${THREADS}" "${VOUCHER_ID}"

  # resetOffsetByTime 会把每个队列的最后一个旧消息留在待消费位置；
  # 先启动一次应用把这点尾巴消费掉，再停应用做 SKIP_BROKER_RESET 的干净 prepare，
  # 否则最后一波旧订单会在正式压测开始后落库，基线断言直接失败。
  echo "-- [2/7] reset broker + 消费尾部旧消息 --"
  bash "${STRESS_DIR}/reset_broker.sh"
  start_app "drain-${label}" "${JDK21_JAR}" "${JAVA21_BIN}" "${port}"
  echo "  等待 ${DRAIN_BROKER_SECONDS}s 让尾部消息消费完..."
  sleep "${DRAIN_BROKER_SECONDS}"
  stop_all_apps

  echo "-- [3/7] prepare（应用已停止，SKIP_BROKER_RESET=1） --"
  SKIP_BROKER_RESET=1 bash "${STRESS_DIR}/prepare_seckill_test.sh" "${THREADS}" "${VOUCHER_ID}"

  echo "-- [4/7] 启动正式应用（benchmark profile + Hikari 20 + INFO） --"
  start_app "${label}" "${jar}" "${java_bin}" "${port}" "$@"

  if [ "${WARMUP_COUNT}" -gt 0 ]; then
    echo "-- [5/7] seckill 预热（券 ${WARMUP_VOUCHER_ID} × ${WARMUP_COUNT}，不碰压测券 ${VOUCHER_ID}） --"
    APP_PORT="${port}" \
      bash "${STRESS_DIR}/warmup_seckill_test.sh" "${WARMUP_COUNT}" "${WARMUP_VOUCHER_ID}"
  else
    echo "-- [5/7] 跳过 seckill 预热（WARMUP_COUNT=0） --"
  fi

  echo "-- [6/7] 商详预热（/shop/1 x20，不消耗券） --"
  local i
  for i in $(seq 1 20); do
    curl --max-time 2 -s -o /dev/null "http://127.0.0.1:${port}/shop/1" || true
  done

  case "${label}" in
    *virtual*)
      grep -q "virtual threads with semaphore" "${APP_HOME}/app-${label}.log" \
        || fail "虚拟线程开关未生效：日志中没有 'virtual threads with semaphore'"
      echo "  已确认虚拟线程 + Semaphore 生效"
      ;;
  esac

  echo "-- [7/7] 压测 --"
  local loop_args=()
  if [ "${SUSTAINED_LOOPS}" -gt 0 ]; then
    loop_args=("${SUSTAINED_LOOPS}")
  fi
  APP_PORT="${port}" \
  RUN_LABEL="${label}" \
  BENCHMARK_NOTE="${note}" \
  APP_JAR_SHA256="$(sha256_of "${jar}")" \
  DRAIN_TIMEOUT="${DRAIN_TIMEOUT}" \
  APP_LOG="${APP_HOME}/app-${label}.log" \
    bash "${STRESS_DIR}/run_seckill_test.sh" "${THREADS}" "${RAMPUP}" "${VOUCHER_ID}" \
    ${loop_args[@]+"${loop_args[@]}"}

  stop_all_apps

  # 归集本轮结果：label / 结果目录 / 吞吐 / 一致性 / MQ发送超时
  local safe_label result_dir verdict thr mqfail
  safe_label=$(printf '%s' "${label}" | tr -cd '[:alnum:]_-')
  result_dir=$(ls -dt "${STRESS_DIR}/results/"*"_${safe_label}" 2>/dev/null | head -1 || true)
  verdict="NO_SUMMARY"
  thr="N/A"
  mqfail="N/A"
  if [ -n "${result_dir}" ] && [ -f "${result_dir}/summary.txt" ]; then
    verdict=$(awk '/一致性判定/{print $NF; exit}' "${result_dir}/summary.txt" 2>/dev/null || echo NO_SUMMARY)
    thr=$(awk '/吞吐量/{print $2; exit}' "${result_dir}/summary.txt" 2>/dev/null || echo N/A)
    mqfail=$(awk '/MQ发送超时/{print $NF; exit}' "${result_dir}/summary.txt" 2>/dev/null || echo N/A)
  fi
  printf '%s\t%s\t%s\t%s\t%s\n' "${label}" "${result_dir##*/}" "${thr}" "${verdict}" "${mqfail}" >> "${RESULTS_FILE}"
  echo "-- ${label} 完成（一致性=${verdict}，MQ发送超时=${mqfail}） --"
}

run_column() {
  local label=$1 jar=$2 java_bin=$3 port=$4 sha=$5 note=$6
  shift 6

  if [ "${SUSTAINED_LOOPS}" -gt 0 ]; then
    label="${label}-x${SUSTAINED_LOOPS}"
    note="${note}; sustained ${SUSTAINED_LOOPS} loops"
  fi

  echo ""
  echo "============================================================"
  echo "  ${label}（${REPEATS} 轮）"
  echo "  jar:    ${jar}"
  echo "  sha256: ${sha}"
  echo "  port:   ${port}"
  echo "  note:   ${note}"
  echo "============================================================"

  local r
  for r in $(seq 1 "${REPEATS}"); do
    echo ""
    echo ">>>>> ${label} 第 ${r}/${REPEATS} 轮 <<<<<"
    run_once "${label}-r${r}" "${jar}" "${java_bin}" "${port}" "${note}" "$@"
  done
}

# ---- 0. 前置检查 ----
echo "============================================================"
echo "  三列 R1 对照：${THREADS} 并发 / ${RAMPUP}s 爬坡 / 券 ${VOUCHER_ID}"
echo "  每列 ${REPEATS} 轮，seckill 预热券 ${WARMUP_VOUCHER_ID} × ${WARMUP_COUNT}"
echo "  APP_HOME: ${APP_HOME}"
echo "============================================================"

[ -f "${STRESS_DIR}/prepare_seckill_test.sh" ] || fail "缺少 prepare_seckill_test.sh"
[ -f "${STRESS_DIR}/run_seckill_test.sh" ] || fail "缺少 run_seckill_test.sh"
[ -f "${STRESS_DIR}/reset_broker.sh" ] || fail "缺少 reset_broker.sh"
[ -f "${STRESS_DIR}/warmup_seckill_test.sh" ] || fail "缺少 warmup_seckill_test.sh"
[ -f "${STRESS_DIR}/parse_jtl.py" ] || fail "缺少 parse_jtl.py"
[ -f "${ENV_FILE}" ] || fail "缺少环境文件: ${ENV_FILE}"
[ -f "${JDK8_JAR}" ] || fail "缺少 JDK8 jar: ${JDK8_JAR}（可用 JDK8_JAR=... 覆盖）"
[ -f "${JDK21_JAR}" ] || fail "缺少 JDK21 jar: ${JDK21_JAR}（可用 JDK21_JAR=... 覆盖）"
command -v python3 >/dev/null 2>&1 || fail "缺少 python3（parse_jtl.py 需要）"
command -v sha256sum >/dev/null 2>&1 || fail "缺少 sha256sum"

if [ -z "${JAVA8_BIN}" ]; then
  JAVA8_BIN=$(find_java "${HOME}/jdk8/bin/java" \
                        /usr/lib/jvm/java-8-openjdk*/bin/java \
                        /usr/lib/jvm/java-1.8.0-openjdk*/bin/java || true)
fi
if [ -z "${JAVA21_BIN}" ]; then
  JAVA21_BIN=$(find_java /usr/lib/jvm/java-21-openjdk*/bin/java || true)
fi
if [ -z "${JAVA21_BIN}" ]; then
  JAVA21_BIN=$(command -v java || true)
fi

[ -n "${JAVA8_BIN}" ] || fail "找不到 JDK8 java，可用 JAVA8_BIN=... 指定"
[ -n "${JAVA21_BIN}" ] || fail "找不到 JDK21 java，可用 JAVA21_BIN=... 指定"
"${JAVA8_BIN}" -version 2>&1 | grep -q 'version "1\.8' \
  || fail "${JAVA8_BIN} 不是 JDK8: $("${JAVA8_BIN}" -version 2>&1 | head -1)"
"${JAVA21_BIN}" -version 2>&1 | grep -q 'version "21' \
  || fail "${JAVA21_BIN} 不是 JDK21: $("${JAVA21_BIN}" -version 2>&1 | head -1)"

# shellcheck disable=SC1090
set -a
. "${ENV_FILE}"
set +a

: "${MYSQL_ROOT_PASSWORD:?${ENV_FILE} 中缺少 MYSQL_ROOT_PASSWORD}"

export MYSQL_PASS="${MYSQL_ROOT_PASSWORD}"
export DB_HOST=${DB_HOST:-127.0.0.1}
export DB_PORT=${DB_PORT:-3306}
export DB_NAME=${DB_NAME:-dingping}
export DB_USER=${DB_USER:-root}
export DB_PASSWORD="${MYSQL_ROOT_PASSWORD}"
export REDIS_HOST=${REDIS_HOST:-127.0.0.1}
export REDIS_PORT=${REDIS_PORT:-6379}
export ROCKETMQ_NAMESRV=${ROCKETMQ_NAMESRV:-127.0.0.1:9876}

JDK8_SHA=$(sha256_of "${JDK8_JAR}")
JDK21_SHA=$(sha256_of "${JDK21_JAR}")

echo "  JDK8:   ${JAVA8_BIN}"
echo "  JDK21:  ${JAVA21_BIN}"
echo "  JDK8 jar sha256:  ${JDK8_SHA}"
echo "  JDK21 jar sha256: ${JDK21_SHA}"
if [ "${SUSTAINED_LOOPS}" -gt 0 ]; then
  echo "  持续并发模式: 每线程 ${SUSTAINED_LOOPS} 循环，JMeter 线程数 $((THREADS / SUSTAINED_LOOPS))"
fi

run_column_by_name() {
  case "$1" in
    col1)
      run_column "col1-jdk8" "${JDK8_JAR}" "${JAVA8_BIN}" "${COL1_PORT}" "${JDK8_SHA}" \
        "JDK8 + platform threads; Hikari 20; com.hmdp INFO"
      ;;
    col2)
      run_column "col2-jdk21-platform" "${JDK21_JAR}" "${JAVA21_BIN}" "${COL2_PORT}" "${JDK21_SHA}" \
        "JDK21 + platform threads; Hikari 20; com.hmdp INFO" \
        --concurrency.virtual-threads.enabled=false \
        --spring.threads.virtual.enabled=false
      ;;
    col3)
      run_column "col3-jdk21-virtual" "${JDK21_JAR}" "${JAVA21_BIN}" "${COL3_PORT}" "${JDK21_SHA}" \
        "JDK21 + virtual threads (Semaphore 8); Hikari 20; com.hmdp INFO" \
        --concurrency.virtual-threads.enabled=true \
        --spring.threads.virtual.enabled=true
      ;;
    *)
      fail "未知列名: $1（可用值 col1 col2 col3）"
      ;;
  esac
}

for col in ${COLUMNS}; do
  run_column_by_name "${col}"
done

echo ""
echo "============================================================"
echo "  ALL COLUMNS DONE — 结果汇总（label / 目录 / 吞吐 / 一致性 / MQ发送超时）"
echo "============================================================"
if command -v column >/dev/null 2>&1; then
  column -t -s "$(printf '\t')" "${RESULTS_FILE}"
else
  cat "${RESULTS_FILE}"
fi
FAILS=$(awk -F'\t' '$4 != "YES" {c++} END {print c+0}' "${RESULTS_FILE}")
echo "  一致性未通过的轮次: ${FAILS}"
echo "  最新结果目录:"
ls -dt "${STRESS_DIR}"/results/*_col* 2>/dev/null | head -"${REPEATS}" || true
echo "  内存快照:"
free -m | sed -n '1,2p'
echo "============================================================"

if [ "${FAILS}" -ne 0 ]; then
  fail "存在一致性未通过的轮次（${FAILS} 轮），数据不可用于跨列对比，请按 ANALYSIS 文档排查"
fi
