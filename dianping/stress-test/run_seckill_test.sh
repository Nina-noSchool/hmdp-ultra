#!/bin/bash
# ============================================================================
# 秒杀Plus压测执行脚本
# 用法: MYSQL_PASS=<密码> ./run_seckill_test.sh [并发数] [启动秒数] [券ID]
# 默认: 1000并发, 2s启动, 券ID=13
# 依赖: 环境变量 MYSQL_PASS 必须已设置（凭据不落盘、不进仓库）
# ============================================================================
set -e

THREADS=${1:-1000}
RAMPUP=${2:-2}
VOUCHER_ID=${3:-13}
APP_PORT=${APP_PORT:-8081}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JMETER_HOME="${HOME}/java项目/apache-jmeter-5.6.3"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_DIR="${SCRIPT_DIR}/results/${TIMESTAMP}"
CSV_FILE="/tmp/seckill_tokens.csv"
JMX_FILE="/tmp/seckill_plus_test.jmx"
MYSQL_PASS="${MYSQL_PASS:?MYSQL_PASS 未设置：请先 export MYSQL_PASS=<密码> 再运行}"

# 确保结果目录存在
mkdir -p "${RESULT_DIR}/report"

echo "=========================================="
echo "  秒杀Plus压测"
echo "  并发数: ${THREADS}"
echo "  启动时间: ${RAMPUP}s"
echo "  券ID: ${VOUCHER_ID}"
echo "  结果目录: ${RESULT_DIR}"
echo "=========================================="

# ---- 1. 环境检查 ----
echo ""
echo "[0/4] 环境检查..."

# 检查应用是否运行
 APP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://localhost:${APP_PORT}/voucher-order/seckill-plus/${VOUCHER_ID}" 2>/dev/null || echo "000")
if [ "$APP_STATUS" = "000" ]; then
  echo "  [FAIL] 应用未启动，请先启动 spring-boot:run"
  exit 1
fi
echo "  应用状态: HTTP ${APP_STATUS}"

# 检查 RocketMQ broker
if ! ps aux | grep -q "[o]rg.apache.rocketmq.broker"; then
  echo "  [WARN] RocketMQ Broker 未运行，MQ发送将全部失败"
fi

# 检查 Redis
if ! redis-cli ping > /dev/null 2>&1; then
  echo "  [FAIL] Redis 未启动"
  exit 1
fi
echo "  Redis: OK"

# ---- 2. 生成JMeter测试计划 ----
echo "[1/4] 生成JMeter测试计划..."
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

      <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="秒杀Plus-${THREADS}并发${RAMPUP}s" enabled="true">
        <stringProp name="ThreadGroup.on_sample_error">continue</stringProp>
        <elementProp name="ThreadGroup.main_controller" elementType="LoopController" guiclass="LoopControlPanel" testclass="LoopController" testname="Loop Controller" enabled="true">
          <boolProp name="LoopController.continue_forever">false</boolProp>
          <stringProp name="LoopController.loops">1</stringProp>
        </elementProp>
        <stringProp name="ThreadGroup.num_threads">${THREADS}</stringProp>
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
          <stringProp name="HTTPSampler.domain">localhost</stringProp>
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

# ---- 3. 准备环境 ----
echo "[2/4] 准备测试数据..."
bash "${SCRIPT_DIR}/prepare_seckill_test.sh" ${THREADS} ${VOUCHER_ID}

# ---- 4. 执行压测 ----
echo ""
echo "[3/4] 开始压测..."
echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"

START_TS=$(date +%s)
${JMETER_HOME}/bin/jmeter -n -t ${JMX_FILE} \
  -l ${RESULT_DIR}/result.jtl \
  -e -o ${RESULT_DIR}/report 2>&1 | tee ${RESULT_DIR}/jmeter.log
END_TS=$(date +%s)
DURATION=$((END_TS - START_TS))

# ---- 5. JMeter 结果汇总 ----
echo ""
echo "[4/5] JMeter 结果汇总..."
echo ""

REDIS_CLI_CMD=$(which redis-cli 2>/dev/null || echo "redis-cli")
REDIS_STOCK=$(${REDIS_CLI_CMD} get "seckill:stock:${VOUCHER_ID}" 2>/dev/null || echo "N/A")
REDIS_ORDERS=$(${REDIS_CLI_CMD} scard "seckill:order:${VOUCHER_ID}" 2>/dev/null || echo "N/A")
DB_ORDERS=$(mysql -u"${MYSQL_USER:-root}" -p"${MYSQL_PASS}" -D dingping -N -e "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id = ${VOUCHER_ID}" 2>/dev/null || echo "N/A")
DB_STOCK=$(mysql -u"${MYSQL_USER:-root}" -p"${MYSQL_PASS}" -D dingping -N -e "SELECT stock FROM tb_seckill_voucher WHERE voucher_id = ${VOUCHER_ID}" 2>/dev/null || echo "N/A")

# 从JMeter summary提取关键指标
JMETER_SUMMARY=$(grep "summary =" ${RESULT_DIR}/jmeter.log | tail -1)
THROUGHPUT=$(echo "${JMETER_SUMMARY}" | grep -o '[0-9.]*/s' | head -1)
AVG_RT=$(echo "${JMETER_SUMMARY}" | grep -o 'Avg: [0-9]*' | awk '{print $2}')
ERR_COUNT=$(echo "${JMETER_SUMMARY}" | grep -o 'Err: *[0-9]*' | awk '{print $2}' | tr -d '(')

cat > ${RESULT_DIR}/summary.txt <<EOF
========================================
  秒杀Plus压测结果
  时间: $(date '+%Y-%m-%d %H:%M:%S')
========================================

测试参数:
  并发数:     ${THREADS}
  启动时间:   ${RAMPUP}s
  券ID:       ${VOUCHER_ID}
  总耗时:     ${DURATION}s

JMeter 指标:
  吞吐量:     ${THROUGHPUT}
  平均响应:   ${AVG_RT}ms
  HTTP错误:   ${ERR_COUNT}

业务指标:
  Redis剩余库存:  ${REDIS_STOCK}
  Redis订单集合:  ${REDIS_ORDERS}
  DB实际订单数:   ${DB_ORDERS}
  DB剩余库存:     ${DB_STOCK}
  Lua层成功率:    $((THREADS - REDIS_STOCK + DB_STOCK))/${THREADS}

详细报告: ${RESULT_DIR}/report/index.html
========================================
EOF

cat ${RESULT_DIR}/summary.txt

# ---- 6. Prometheus 指标报告 ----
echo ""
echo "[5/5] 生成 Prometheus 指标报告..."
START_MS=$((START_TS * 1000))
END_MS=$((END_TS * 1000))
python3 "${SCRIPT_DIR}/generate_metrics_report.py" ${START_MS} ${END_MS} "${RESULT_DIR}"
echo "  指标报告: ${RESULT_DIR}/metrics-report.html"