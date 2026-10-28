#!/bin/bash
# ============================================================================
# 秒杀Plus压测执行脚本
# 用法: MYSQL_PASS=<密码> ./run_seckill_test.sh [并发数] [启动秒数] [券ID]
# 默认: 1000并发, 2s启动, 券ID=13
# 依赖: 环境变量 MYSQL_PASS 必须已设置（凭据不落盘、不进仓库）
# ============================================================================
set -euo pipefail

THREADS=${1:-1000}
RAMPUP=${2:-2}
VOUCHER_ID=${3:-13}
# 可选第 4 参：每个 JMeter 线程的循环次数（默认 1 = 旧行为，每线程一单）。
# LOOPS>1 时实际 JMeter 线程数 = THREADS/LOOPS，形成"持续并发"负载
# （总请求数、库存、token 数仍为 THREADS；CSV recycle=false + stopThread 精确消耗）。
# 动机：4000 线程 ramp-up 模式下，JMeter 的建线程速率（~125/s）会成为假瓶颈。
LOOPS=${4:-1}
if [ "${LOOPS}" -lt 1 ]; then
  echo "[FAIL] LOOPS 必须 >= 1" >&2
  exit 1
fi
if [ "${LOOPS}" -gt 1 ] && [ $((THREADS % LOOPS)) -ne 0 ]; then
  echo "[FAIL] THREADS(${THREADS}) 必须能被 LOOPS(${LOOPS}) 整除" >&2
  exit 1
fi
JM_THREADS=${THREADS}
if [ "${LOOPS}" -gt 1 ]; then
  JM_THREADS=$((THREADS / LOOPS))
fi
APP_PORT=${APP_PORT:-8081}
RUN_LABEL=${RUN_LABEL:-}
SAFE_LABEL=$(echo "${RUN_LABEL}" | tr -cd '[:alnum:]_-')
BENCHMARK_NOTE=${BENCHMARK_NOTE:-}
APP_JAR_SHA256=${APP_JAR_SHA256:-}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JMETER_HOME=${JMETER_HOME:-"${HOME}/java项目/apache-jmeter-5.6.3"}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_DIR="${SCRIPT_DIR}/results/${TIMESTAMP}${SAFE_LABEL:+_${SAFE_LABEL}}"
CSV_FILE=${CSV_FILE:-"/tmp/seckill_tokens.csv"}
JMX_FILE=${JMX_FILE:-"/tmp/seckill_plus_test.jmx"}
MYSQL_HOST=${MYSQL_HOST:-127.0.0.1}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_PASS="${MYSQL_PASS:?MYSQL_PASS 未设置：请先 export MYSQL_PASS=<密码> 再运行}"
MYSQL_DB=${MYSQL_DB:-dingping}
MYSQL_CONTAINER=${MYSQL_CONTAINER:-hmdp-mysql}
REDIS_HOST=${REDIS_HOST:-127.0.0.1}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_PASSWORD=${REDIS_PASSWORD:-}
JMETER_HEAP=${JMETER_HEAP:-"-Xms2g -Xmx2g -XX:MaxMetaspaceSize=256m"}
JMETER_NICE=${JMETER_NICE:-19}
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-120}
APP_LOG=${APP_LOG:-}

export MYSQL_PWD="${MYSQL_PASS}"
# 与 prepare 相同：VM 的 mysql 是容器包装脚本，必须显式转发 MYSQL_PWD。
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

[ -x "${JMETER_HOME}/bin/jmeter" ] || { echo "[FAIL] JMeter 不存在: ${JMETER_HOME}/bin/jmeter" >&2; exit 1; }

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

# 确保结果目录存在
mkdir -p "${RESULT_DIR}/report"

echo "=========================================="
echo "  秒杀Plus压测"
echo "  并发数: ${THREADS}"
echo "  启动时间: ${RAMPUP}s"
echo "  券ID: ${VOUCHER_ID}"
echo "  结果目录: ${RESULT_DIR}"
echo "=========================================="

# ---- 1. 环境检查 + 基线断言（不再自动 prepare） ----
echo ""
echo "[0/6] 环境检查与基线断言..."

HEALTH=$(curl --max-time 3 -s "http://127.0.0.1:${APP_PORT}/actuator/health" || true)
echo "${HEALTH}" | grep -q '"status":"UP"' || fail "应用未就绪: ${HEALTH}"

APP_COUNT=$(ps -eo comm=,args= | awk '$1 == "java" && $0 ~ /hmdp.*\.jar/ {count++} END {print count+0}')
[ "${APP_COUNT}" = "1" ] || fail "检测到 ${APP_COUNT} 个应用实例，必须恰好 1 个（两个实例会瓜分 consumer group）"

FLAGS=$(curl --max-time 3 -s "http://127.0.0.1:${APP_PORT}/jvm/seckill-bug/status" || true)
echo "${FLAGS}" | grep -q '"success":true' || echo "[WARN] 无法读取故障注入开关状态"
if echo "${FLAGS}" | grep -qE '"(cpuRegex|cpuSpin|memoryLeak|threadlocalLeak|deadlock|threadExplosion|gcAlloc)":true'; then
  fail "故障注入开关未全部关闭: ${FLAGS}"
fi

PONG=$("${REDIS_CMD[@]}" ping)
[ "${PONG}" = "PONG" ] || fail "Redis ping 失败: ${PONG}"
"${MYSQL_CMD[@]}" -e "SELECT 1" >/dev/null
echo "  应用: UP (port ${APP_PORT})，实例数: ${APP_COUNT}，Redis/MySQL: OK"

# 基线必须是 prepare 后的干净状态；不干净时拒绝压测，避免 2957 脏数据类事故
DB_ORDERS=$("${MYSQL_CMD[@]}" -e "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id = ${VOUCHER_ID}")
DB_STOCK=$("${MYSQL_CMD[@]}" -e "SELECT stock FROM tb_seckill_voucher WHERE voucher_id = ${VOUCHER_ID}")
REDIS_STOCK=$("${REDIS_CMD[@]}" get "seckill:stock:${VOUCHER_ID}")
REDIS_ORDERS=$("${REDIS_CMD[@]}" scard "seckill:order:${VOUCHER_ID}")
TOKEN_COUNT=$("${REDIS_CMD[@]}" --scan --pattern 'login:token:tokentest*' | wc -l | tr -d ' ')
CSV_LINES=$(wc -l < "${CSV_FILE}" 2>/dev/null | tr -d ' ' || echo 0)

[ "${DB_ORDERS}" = "0" ] || fail "DB 订单数=${DB_ORDERS}，基线不干净：请先停应用 → prepare → 启动应用 → 再压测"
[ "${REDIS_ORDERS}" = "0" ] || fail "Redis 订单集合=${REDIS_ORDERS}，基线不干净：请先 prepare"
[ "${DB_STOCK}" = "${THREADS}" ] || fail "DB 库存=${DB_STOCK}，期望 ${THREADS}：请先 prepare"
[ "${REDIS_STOCK}" = "${THREADS}" ] || fail "Redis 库存=${REDIS_STOCK}，期望 ${THREADS}：请先 prepare"
[ "${TOKEN_COUNT}" -ge "${THREADS}" ] || fail "token 数=${TOKEN_COUNT}，少于并发数 ${THREADS}"
[ "${CSV_LINES}" -ge "${THREADS}" ] || fail "CSV 行数=${CSV_LINES}，少于并发数 ${THREADS}"
echo "  基线: DB订单=0 Redis订单=0 DB库存=${DB_STOCK} Redis库存=${REDIS_STOCK} tokens=${TOKEN_COUNT}"

# ---- 2. 生成JMeter测试计划 ----
echo "[1/6] 生成JMeter测试计划..."
cat > ${JMX_FILE} <<JMXEOF
<?xml version="1.0" encoding="UTF-8"?>
<jmeterTestPlan version="1.2" properties="5.0" jmeter="5.6.3">
  <hashTree>
    <TestPlan guiclass="TestPlanGui" testclass="TestPlan" testname="秒杀Plus压测" enabled="true">
      <stringProp name="TestPlan.comments">${THREADS}并发 ${RAMPUP}s启动</stringProp>
      <boolProp name="TestPlan.functional_mode">false</boolProp>
      <boolProp name="TestPlan.tearDown_on_shutdown">true</boolProp>
      <boolProp name="TestPlan.serialize_threadgroups">false</boolProp>
      <elementProp name="TestPlan.user_defined_variables" elementType="Arguments" guiclass="ArgumentsPanel" testclass="Arguments" testname="User Defined Variables" enabled="true">
        <collectionProp name="Arguments.arguments"/>
      </elementProp>
    </TestPlan>
    <hashTree>
      <CSVDataSet guiclass="TestBeanGUI" testclass="CSVDataSet" testname="CSV - Token" enabled="true">
        <stringProp name="filename">${CSV_FILE}</stringProp>
        <stringProp name="fileEncoding">UTF-8</stringProp>
        <stringProp name="variableNames">token</stringProp>
        <boolProp name="ignoreFirstLine">false</boolProp>
        <stringProp name="delimiter">,</stringProp>
        <boolProp name="quotedData">false</boolProp>
        <boolProp name="recycle">false</boolProp>
        <boolProp name="stopThread">true</boolProp>
        <stringProp name="shareMode">shareMode.all</stringProp>
      </CSVDataSet>
      <hashTree/>

      <ResultCollector guiclass="SummaryReport" testclass="ResultCollector" testname="Summary Report" enabled="true">
        <boolProp name="ResultCollector.error_logging">false</boolProp>
        <objProp>
          <name>saveConfig</name>
          <value class="SampleSaveConfiguration">
            <time>true</time><latency>true</latency><timestamp>true</timestamp>
            <success>true</success><label>true</label><code>true</code>
            <message>true</message><threadName>true</threadName><dataType>true</dataType>
            <encoding>false</encoding><assertions>true</assertions><subresults>true</subresults>
            <responseData>false</responseData><samplerData>false</samplerData>
            <xml>false</xml><fieldNames>true</fieldNames>
            <responseHeaders>false</responseHeaders><requestHeaders>false</requestHeaders>
            <responseDataOnError>false</responseDataOnError>
            <saveAssertionResultsFailureMessage>true</saveAssertionResultsFailureMessage>
            <assertionsResultsToSave>0</assertionsResultsToSave>
            <bytes>true</bytes><sentBytes>true</sentBytes><url>true</url>
            <threadCounts>true</threadCounts><idleTime>true</idleTime><connectTime>true</connectTime>
          </value>
        </objProp>
        <stringProp name="filename">${RESULT_DIR}/result.jtl</stringProp>
      </ResultCollector>
      <hashTree/>

      <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="秒杀Plus-${JM_THREADS}并发x${LOOPS}循环${RAMPUP}s" enabled="true">
        <stringProp name="ThreadGroup.on_sample_error">continue</stringProp>
        <elementProp name="ThreadGroup.main_controller" elementType="LoopController" guiclass="LoopControlPanel" testclass="LoopController" testname="Loop Controller" enabled="true">
          <boolProp name="LoopController.continue_forever">false</boolProp>
          <stringProp name="LoopController.loops">${LOOPS}</stringProp>
        </elementProp>
        <stringProp name="ThreadGroup.num_threads">${JM_THREADS}</stringProp>
        <stringProp name="ThreadGroup.ramp_time">${RAMPUP}</stringProp>
        <boolProp name="ThreadGroup.scheduler">false</boolProp>
        <stringProp name="ThreadGroup.duration"></stringProp>
        <stringProp name="ThreadGroup.delay"></stringProp>
        <boolProp name="ThreadGroup.same_user_on_next_iteration">true</boolProp>
      </ThreadGroup>
      <hashTree>
        <HTTPSamplerProxy guiclass="HttpTestSampleGui" testclass="HTTPSamplerProxy" testname="seckill-plus/${VOUCHER_ID}" enabled="true">
          <elementProp name="HTTPsampler.Arguments" elementType="Arguments" guiclass="HTTPArgumentsPanel" testclass="Arguments" testname="User Defined Variables" enabled="true">
            <collectionProp name="Arguments.arguments"/>
          </elementProp>
          <stringProp name="HTTPSampler.domain">127.0.0.1</stringProp>
          <stringProp name="HTTPSampler.port">${APP_PORT}</stringProp>
          <stringProp name="HTTPSampler.protocol">http</stringProp>
          <stringProp name="HTTPSampler.contentEncoding">UTF-8</stringProp>
          <stringProp name="HTTPSampler.path">/voucher-order/seckill-plus/${VOUCHER_ID}</stringProp>
          <stringProp name="HTTPSampler.method">POST</stringProp>
          <boolProp name="HTTPSampler.follow_redirects">true</boolProp>
          <boolProp name="HTTPSampler.auto_redirects">false</boolProp>
          <boolProp name="HTTPSampler.use_keepalive">true</boolProp>
          <boolProp name="HTTPSampler.DO_MULTIPART_POST">false</boolProp>
          <stringProp name="HTTPSampler.embedded_url_re"></stringProp>
          <stringProp name="HTTPSampler.connect_timeout"></stringProp>
          <stringProp name="HTTPSampler.response_timeout"></stringProp>
        </HTTPSamplerProxy>
        <hashTree>
          <HeaderManager guiclass="HeaderPanel" testclass="HeaderManager" testname="Auth Header" enabled="true">
            <collectionProp name="HeaderManager.headers">
              <elementProp name="" elementType="Header">
                <stringProp name="Header.name">authorization</stringProp>
                <stringProp name="Header.value">\${token}</stringProp>
              </elementProp>
              <elementProp name="" elementType="Header">
                <stringProp name="Header.name">Content-Type</stringProp>
                <stringProp name="Header.value">application/json</stringProp>
              </elementProp>
            </collectionProp>
          </HeaderManager>
          <hashTree/>
        </hashTree>
      </hashTree>
    </hashTree>
  </hashTree>
</jmeterTestPlan>
JMXEOF
echo "  测试计划: ${JMX_FILE}"

# ---- 3. 不再自动 prepare ----
echo "[2/6] 基线已由 prepare 完成（本脚本不再清库，避免与运行中的应用 Job/消费者竞争）"

# ---- 4. 执行压测 ----
echo ""
echo "[3/6] 开始压测..."
echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"

START_TS=$(date +%s)
# JMeter 自身也吃内存：4000 线程时默认 1G 堆容易 GC 抖动；HEAP 由 jmeter 启动脚本读取
export HEAP="${JMETER_HEAP}"
# JMeter 与被测应用同机时，4000 线程会和应用/broker 抢 CPU；
# 把 JMeter 降到低优先级，让被测进程优先拿到 CPU，减少 syncSend 超时假阳性。
nice -n "${JMETER_NICE}" "${JMETER_HOME}/bin/jmeter" -n -t "${JMX_FILE}" \
  -l "${RESULT_DIR}/result.jtl" \
  -e -o "${RESULT_DIR}/report" 2>&1 | tee "${RESULT_DIR}/jmeter.log"
END_TS=$(date +%s)

# ---- 5. 等待消费端收敛（JMeter 返回只代表 HTTP 完成，建单是异步的） ----
echo ""
echo "[4/6] 等待消费端落库（最多 ${DRAIN_TIMEOUT}s）..."
REDIS_STOCK=$("${REDIS_CMD[@]}" get "seckill:stock:${VOUCHER_ID}" || echo "N/A")
EXPECTED_ORDERS=""
if [[ "${REDIS_STOCK}" =~ ^[0-9]+$ ]]; then
  EXPECTED_ORDERS=$((THREADS - REDIS_STOCK))
fi

DRAIN_STATUS="SKIPPED"
if [ -z "${EXPECTED_ORDERS}" ]; then
  echo "[WARN] Redis 库存不可读，跳过收敛等待"
else
  DRAIN_STATUS="TIMEOUT"
  DRAIN_START=$(date +%s)
  while true; do
    DB_ORDERS=$("${MYSQL_CMD[@]}" -e "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id = ${VOUCHER_ID}")
    if [ "${DB_ORDERS}" = "${EXPECTED_ORDERS}" ]; then
      DRAIN_STATUS="DRAINED"
      echo "  消费已收敛：DB 订单=${DB_ORDERS}（Redis 成功=${EXPECTED_ORDERS}）"
      break
    fi
    if [ $(( $(date +%s) - DRAIN_START )) -ge "${DRAIN_TIMEOUT}" ]; then
      echo "[WARN] 等待收敛超时：DB 订单=${DB_ORDERS}，期望=${EXPECTED_ORDERS}（结果按当前状态记录）"
      break
    fi
    sleep 2
  done
fi

# ---- 6. 结果汇总（以 result.jtl 为准，不解析 summariser 文本） ----
echo ""
echo "[5/6] 结果汇总..."
echo ""

python3 "${SCRIPT_DIR}/parse_jtl.py" "${RESULT_DIR}/result.jtl" "${RESULT_DIR}/metrics.env"
# shellcheck disable=SC1090
source "${RESULT_DIR}/metrics.env"

REDIS_STOCK=$("${REDIS_CMD[@]}" get "seckill:stock:${VOUCHER_ID}" || echo "N/A")
REDIS_ORDERS=$("${REDIS_CMD[@]}" scard "seckill:order:${VOUCHER_ID}" || echo "N/A")
DB_ORDERS=$("${MYSQL_CMD[@]}" -e "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id = ${VOUCHER_ID}" || echo "N/A")
DB_STOCK=$("${MYSQL_CMD[@]}" -e "SELECT stock FROM tb_seckill_voucher WHERE voucher_id = ${VOUCHER_ID}" || echo "N/A")

# Redis 是同步预扣，Lua 成功数 = 并发数 - Redis 剩余库存
LUA_SUCCESS="N/A"
if [[ "${REDIS_STOCK}" =~ ^[0-9]+$ ]]; then
  LUA_SUCCESS=$((THREADS - REDIS_STOCK))
fi

CONSISTENT="NO"
if [ "${TOTAL}" = "${THREADS}" ] && [ "${ERRORS}" = "0" ] \
   && [ "${DRAIN_STATUS}" = "DRAINED" ] \
   && [ "${DB_ORDERS}" = "${THREADS}" ] && [ "${REDIS_ORDERS}" = "${THREADS}" ] \
   && [ "${DB_STOCK}" = "0" ] && [ "${REDIS_STOCK}" = "0" ]; then
  CONSISTENT="YES"
fi

# 应用日志错误计数（自应用启动，含预热阶段）——HTTP 200 不代表链路健康，
# MQ 超时回滚（syncSend failed）就藏在业务失败体里，必须显式数出来。
MQ_SEND_FAIL="(未采集)"
MQ_PUBLISH_FAIL="-"
LOCK_WAIT="-"
DB_STOCK_SHORT="-"
RATE_LIMIT_HIT="-"
if [ -n "${APP_LOG}" ] && [ -f "${APP_LOG}" ]; then
  MQ_SEND_FAIL=$(grep -c 'syncSend failed' "${APP_LOG}" 2>/dev/null || true)
  MQ_PUBLISH_FAIL=$(grep -c 'rocketmq publish failed' "${APP_LOG}" 2>/dev/null || true)
  LOCK_WAIT=$(grep -c 'Lock wait timeout' "${APP_LOG}" 2>/dev/null || true)
  DB_STOCK_SHORT=$(grep -c 'db stock not enough' "${APP_LOG}" 2>/dev/null || true)
  RATE_LIMIT_HIT=$(grep -c 'sliding window rate limit hit' "${APP_LOG}" 2>/dev/null || true)
fi

cat > "${RESULT_DIR}/summary.txt" <<EOF
========================================
  秒杀Plus压测结果
  时间: $(date '+%Y-%m-%d %H:%M:%S')
  标签: ${RUN_LABEL:-"(none)"}
  配置: ${BENCHMARK_NOTE:-"(none)"}
  jar sha256: ${APP_JAR_SHA256:-"(none)"}
========================================

测试参数:
  总请求:     ${THREADS}
  JMeter线程: ${JM_THREADS} × ${LOOPS} 循环
  启动时间:   ${RAMPUP}s
  券ID:       ${VOUCHER_ID}
  端口:       ${APP_PORT}
  压测窗口:   ${DURATION_S}s

JMeter 指标:
  总请求:     ${TOTAL}
  吞吐量:     ${THROUGHPUT} req/s
  平均响应:   ${AVG} ms
  P50/P95/P99: ${P50} / ${P95} / ${P99} ms
  HTTP错误:   ${ERRORS}

业务指标:
  Redis剩余库存:  ${REDIS_STOCK}
  Redis订单集合:  ${REDIS_ORDERS}
  DB实际订单数:   ${DB_ORDERS}
  DB剩余库存:     ${DB_STOCK}
  Lua层成功率:    ${LUA_SUCCESS}/${THREADS}
  消费收敛:       ${DRAIN_STATUS}
  一致性判定:     ${CONSISTENT}

应用日志错误计数（自启动起，含预热）:
  MQ发送超时:     ${MQ_SEND_FAIL}
  MQ发布失败:     ${MQ_PUBLISH_FAIL}
  行锁等待超时:   ${LOCK_WAIT}
  DB库存不足:     ${DB_STOCK_SHORT}
  限流拒绝:       ${RATE_LIMIT_HIT}

详细报告: ${RESULT_DIR}/report/index.html
========================================
EOF

cat "${RESULT_DIR}/summary.txt"

# ---- 6. Prometheus 指标报告 ----
echo ""
echo "[6/6] 生成 Prometheus 指标报告..."
START_MS=$((START_TS * 1000))
END_MS=$((END_TS * 1000))
python3 "${SCRIPT_DIR}/generate_metrics_report.py" ${START_MS} ${END_MS} "${RESULT_DIR}"
echo "  指标报告: ${RESULT_DIR}/metrics-report.html"