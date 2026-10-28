# 秒杀压测基线（安全版）

这套脚本只改测试流程、配置和索引，**不改 Java 业务代码**。目标是把“跑出来的数字”
变成可复现、可解释的数字：同一台机器、同一份数据基线、同一套连接池/日志级别，
并且压测开始前就能发现脏基线。

## 1. 为什么之前的数字不可直接比较

旧流程有三个会把结果带偏的点：

1. `prepare` 在应用运行时清库。7 个定时 Job 和 MQ 消费者还在跑，清完库马上又写回
   `PENDING_RETRY` / 新订单，导致“prepare 后 DB orders=2957”。
2. RocketMQ 里残留的旧消息（包括 20 分钟延时消息）会在下一轮被重新消费。
3. JMeter 的 Summary 文本是空格对齐的，`grep 'Avg: [0-9]*'` 匹配不到
   `Avg:    53`，旧汇总里的 Avg 实际是空值。

现在的流程把这三件事分别收口：**停应用再 prepare**、**reset broker 位点**、
**从 result.jtl 解析指标**。

## 2. 标准流程

以下命令都在 VM 上执行，`MYSQL_PASS` 从 `~/hmdp-infra/.env` 读取，不写进任何文件。

### 2.1 停应用

```bash
# 确认没有实例；有就先停掉
ps -eo pid,args | grep '[h]mdp-start'
```

`prepare_seckill_test.sh` 默认会拒绝在应用运行时清库。只有显式
`ALLOW_RUNNING_APP=1` 才会放行，**不要用它跑正式数据**。

### 2.2 prepare（应用必须已停止）

```bash
set -a
. ~/hmdp-infra/.env
set +a

cd ~/hmdp-mq/stress-test
export MYSQL_PASS="$MYSQL_ROOT_PASSWORD"

bash prepare_seckill_test.sh 4000 13
```

脚本会自动识别 `hmdp-mysql` 容器：在 VM 上通过
`docker exec -e MYSQL_PWD` 转发密码（VM 的 `/usr/local/bin/mysql` 是容器包装脚本，
不会自动继承宿主机环境变量）；没有该容器时回退到本机 `mysql` + `MYSQL_PWD`。
容器名可用 `MYSQL_CONTAINER` 覆盖。

它会依次做：

- 检查应用实例数为 0；
- 调用 `reset_broker.sh` 把两个消费组的主 topic 和 `%RETRY%<group>`
  重试 topic 都重置到当前时间（只重置主 topic 会漏掉上一轮失败的重试消息）；
- 清 `tb_voucher_order` / `tb_voucher_reconcile_log` /
  `tb_order_create_verify_task` / `tb_order_create_recovery_task` /
  `tb_seckill_reconcile_task` / `tb_mq_event_record`；
- 把 DB/Redis 库存都设为 4000；
- 清理旧 token 和限流 key，重新生成 4000 个 token 与 CSV；
- 逐项断言，最后打印 `PREPARE_OK ...`。任何一项不符都会非零退出。

**reset 会留下每个队列的最后一条消息**：`resetOffsetByTime` 只能把位点推到
“最后一条消息”的位置，不会超过它。所以第一遍 prepare 后，启动应用会消费掉
每个队列最后一条旧消息（实测 200 并发时是 9 条），DB 里会多出几笔旧订单。

安全的收口是两段式：

```bash
# 0) 先做一次 preclean：应用停止，只清库/本地消息表，不 reset
SKIP_BROKER_RESET=1 bash prepare_seckill_test.sh 4000 13

# 1) reset 后，先启动应用把尾部消息消费掉
#    等 health UP 后再等约 15s，然后停应用

# 2) 应用停止后，不 reset，只清库/重置库存
SKIP_BROKER_RESET=1 bash prepare_seckill_test.sh 4000 13

# 3) 再启动正式压测应用
```

preclean 是为了防止上一轮失败留在 `tb_mq_event_record` 的消息，在 drain 应用启动后
被 `MqEventRetryJob` 重新发到 broker。`three_columns_12g.sh` 已经内置了
preclean + reset/drain + 最终 prepare，三列不需要手动做。

跳过 broker 重置只用于“确认没有积压”的首次调试：

```bash
SKIP_BROKER_RESET=1 bash prepare_seckill_test.sh 4000 13
```

### 2.3 启动应用（benchmark profile）

```bash
cd ~/hmdp-mq
nohup java -Xms512m -Xmx1536m -jar hmdp-start-0.0.1-SNAPSHOT.jar \
  --spring.profiles.active=benchmark \
  --spring.datasource.hikari.maximum-pool-size=20 \
  --logging.level.com.hmdp=info \
  > app-benchmark.log 2>&1 &
```

`application-benchmark.yaml` 固定在 `hmdp-start/src/main/resources/`：

```yaml
spring.datasource.hikari.maximum-pool-size: 20
logging.level.com.hmdp: info
```

旧 jar 里可能还没有这个 profile 文件，所以三列脚本同时用命令行参数传同样的值——
命令行优先级更高，效果一致。

### 2.4 跑一轮

```bash
cd ~/hmdp-mq/stress-test
APP_PORT=8081 \
RUN_LABEL=my-run \
BENCHMARK_NOTE="JDK21 platform; Hikari 20; INFO" \
APP_JAR_SHA256="$(sha256sum ~/hmdp-mq/hmdp-start-0.0.1-SNAPSHOT.jar | awk '{print $1}')" \
  bash run_seckill_test.sh 4000 3 13
```

JMeter 默认用 `nice -n 19` 运行（`JMETER_NICE` 可覆盖），让被测应用和 broker
优先拿到 CPU；同机 4000 线程时这能减少“JMeter 自己把 CPU 抢光”造成的假超时。

`run_seckill_test.sh` 现在只做执行和校验，**不会自动 prepare**。它会在开始前断言：

- `/actuator/health` 为 UP；
- 恰好 1 个 `hmdp-start` 进程；
- 7 个故障注入开关全部关闭；
- Redis/MySQL 可连；
- DB 订单=0、Redis 订单=0、DB/Redis 库存=4000、token/CSV ≥ 4000。

任何一条不满足直接退出，不会产生“看起来跑完了”的脏数据。

JMeter 结束后，脚本会等待消费端落库（默认最多 120 秒，`DRAIN_TIMEOUT` 可调），
再以 `result.jtl` 为准生成 `metrics.env` 和 `summary.txt`。

### 2.5 seckill 预热（强烈建议，防冷启动假阳性）

**问题**：冷 JVM + 4000/3s 冲激下，Tomcat 的 800 个工作线程会同时阻塞在
第一批 `syncSend` 上直至超时——老栈（JDK8 + rocketmq-spring 2.1.1）实测
800 单全部回滚，Redis 剩 800、DB 只有 3200，`一致性判定: NO`，整轮作废。
HTTP 层仍是 200，只看错误数发现不了。

**方案**：正式压测前，用独立预热券（默认 999，与压测券 13 隔离）顺序打
300 单，把 MQ producer 连接、JIT、连接池、broker 页缓存焐热：

```bash
# 一次性准备：预热券的行（幂等，只需执行一次）
mysql -uroot -p dingping < ~/hmdp-mq/dianping/hmdp-controller/src/main/resources/db/seed_warmup_voucher.sql

# 每次应用启动后、压测前执行
MYSQL_PASS="$MYSQL_ROOT_PASSWORD" APP_PORT=8082 \
  bash warmup_seckill_test.sh 300 999
```

预热脚本自带防呆：应用必须在线、券行必须存在（忘了跑 seed SQL 会直接报错
而不是 300 单空转）、Lua 成功数必须过半、预热订单必须落库收敛，最后按
payload 清掉预热失败留在 `tb_mq_event_record` 的残留——防止 `MqEventRetryJob`
在正式压测进行中途回放它们制造 DB 噪声。

为什么用独立券而不是券 13：一人一单、请求去重、基线断言都按 voucher_id
隔离，预热数据完全不碰压测券；每轮 prepare 会通过 `EXTRA_VOUCHER_IDS`
把预热券的数据一并清掉。

### 2.6 三列对照

```bash
cd ~/hmdp-mq/stress-test
bash three_columns_12g.sh
```

每列默认跑 `REPEATS=2` 轮，每轮都是完整的
"preclean → reset+drain → prepare → 启动 → seckill 预热 → 压测"循环——
哪怕某轮失败留下本地消息表残留，下一轮的 preclean + broker reset 也会把它清掉，
不会滚雪球。结束时输出结果表（label / 目录 / 吞吐 / 一致性 / MQ发送超时），
任何一轮一致性未通过，脚本整体非零退出。

三列依次是：

| 列 | jar | 线程 | 额外参数 |
|---|---|---|---|
| `col1-jdk8` | `hmdp-start.pre-rerun-20261024.jar` | JDK8 平台线程 | 无 |
| `col2-jdk21-platform` | `hmdp-jdk21-main-saved.jar` | JDK21 平台线程 | `concurrency.virtual-threads.enabled=false` |
| `col3-jdk21-virtual` | `hmdp-jdk21-main-saved.jar` | JDK21 虚拟线程 | 两个开关都 true |

每列都是独立的安全循环：

```
停应用 → prepare（含 broker reset）→ 启动应用 → 预热 → 压测 → 停应用
```

脚本不会用 `cp` 覆盖 canonical jar，避免上一次换 jar 把 `saved` 文件污染。
三列统一 Hikari 20、`com.hmdp=INFO`、堆 1.5G；**JDK21 两列之间**唯一的自变量是
线程模型，col1 是旧栈（JDK8 + 老 RocketMQ 客户端）基线。

结果目录按 label 后缀区分，例如：

```
results/20261027_101500_col3-jdk21-virtual/
  result.jtl
  metrics.env
  summary.txt
  report/index.html
```

> 说明：安全版 prepare 要求应用停止，所以“同一 JVM 连续跑冷/热两轮”不再自动执行。
> 三列脚本每列只跑一轮可比较的数据；如果要看 JVM 预热，建议单独做“同一 JVM
> 连跑两轮但不 prepare”的实验，并在结论里明确它不是库存基线对比。

## 3. JTL 指标解析

`parse_jtl.py` 从 CSV JTL 计算：

- `TOTAL`：样本数
- `ERRORS`：`success != true` 的样本数
- `THROUGHPUT`：`TOTAL / (最后样本结束时间 - 最早样本开始时间)`
- `AVG` / `P50` / `P95` / `P99`：`elapsed` 毫秒
- `DURATION_S`：压测窗口秒数

结果写到 `metrics.env`，可直接 `source`。这个口径与 JMeter Summary 的
“total / duration”一致，但不再依赖空格对齐的日志文本。

## 4. 索引补丁

`hmdp-controller/src/main/resources/db/patch_benchmark_indexes.sql` 是可重复执行的
存储过程版本（MySQL 8 没有 `CREATE INDEX IF NOT EXISTS`）。加索引的依据是实际 SQL：

- `tb_voucher_order`：`create_time` 用于对账扫描，`voucher_id` 用于压测统计；
- `tb_order_create_verify_task` / `tb_order_create_recovery_task`：`voucher_id` 统计；
- `tb_voucher_reconcile_log`：`(log_type, create_time)` 用于 DEDUCT 日志扫描。

执行（按项目约定，DDL 手动执行，不自动跑）：

```bash
mysql -uroot -p dingping < ~/hmdp-mq/dianping/hmdp-controller/src/main/resources/db/patch_benchmark_indexes.sql
```

`hmdp.sql` 里对应的 `CREATE TABLE` 也已同步补上索引，新环境初始化时不需要再跑补丁。

## 5. 已知边界

`reset_broker.sh` 能跳过**已经进入消费队列**的历史消息和 `%RETRY%` 重试消息，
但 RocketMQ 的延时/重试消息在到期前存放在调度队列里，位点重置看不到它们。
如果上一轮压测刚发生过大量消费失败，这些尚未到期的消息可能在下一轮中途重放。

因此：

- 正式三列数据前，确认上一轮没有留下大量 `PENDING_RETRY` 或消费失败；
- 如果刚跑过失败轮次，等过最大重试延迟（或换一个未使用过的券）再跑；
- 脚本的基线断言（开始前 DB orders=0）和一致性判定（结束后三方数字咬合）
  会把这类干扰暴露出来——看到 `一致性判定: NO` 时，不要把它当成运行时性能结论，
  先排查是否有旧消息重放。

另外，**冷 JVM + 4000/3s 冲激**本身也会制造假阳性：老栈（JDK8 + rocketmq-spring
2.1.1）在第一批 800 个并发 `syncSend` 上可能集体超时（`sendDefaultImpl call timeout`），
HTTP 仍是 200，但 800 单会回滚 Redis，最终 `一致性判定: NO`。这不是虚拟线程的
对照数据；正式对比前应先用独立测试券做 seckill 预热，或把 JMeter 移到另一台机器。

## 6. 常见问题

**`prepare` 报“检测到 N 个应用实例仍在运行”**
先停应用。脚本故意不帮你 `kill`，避免误杀别的进程。

**`reset_broker.sh` 报找不到 mqadmin**
确认 `hmdp-mq-broker` 容器在运行；或设置
`MQ_BROKER_CONTAINER` / `MQADMIN_IN_CONTAINER`。宿主机装了 mqadmin 也可以。

**压测前 DB orders 不是 0**
说明 broker 里还有旧消息在重放，或应用启动后消费者先跑了。回到 2.1 重新
“停应用 → prepare → 启动”。不要带着脏基线继续跑。

**summary 里 `消费收敛: TIMEOUT`**
消费端 120 秒内没追平。先看 `app-*.log` 是否有 `Lock wait timeout` /
`db stock not enough`；这种结果不要和干净轮次放在同一张表里比较。

**`metrics-report.html` 是空的**
Prometheus 没启动或没抓到这个端口。业务一致性仍以 `summary.txt` 的三方数字为准。
