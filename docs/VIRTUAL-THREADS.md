# 虚拟线程接入方案（Phase 7，方案 B 后续实验）

> **文档性质**：执行规格书——给执行者（AI 或人）的分步指令，每步有完成标志、验证手段与回滚开关。
> **前置条件**：JDK 21 / Boot 3.2 迁移已完成并合入 main（见 [MIGRATION-JDK21.md](MIGRATION-JDK21.md)）。
> **核心原则**：**一切线程池改动都必须挂在可开关的配置属性后面**，默认关闭——这是本项目的实验纪律
> （对齐 `cache.local.enabled`、限流开关的既有模式），也是压测对照实验能成立的前提。

---

## 0. 目标与边界

**成功的定义（缺一不可）**：
1. 开关关闭时：106 单测全绿、VM 行为与现状分毫不变（回归零）；
2. 开关开启时：VM 冒烟全过 + R1 同机同参数对照压测完成，产出三列对比数据
   （JDK8 基线 / JDK21 平台线程 / JDK21 虚拟线程）；
3. pinning 审计完成：负载下 `jcmd` 线程转储无载体线程钉死堆积；
4. 一份《虚拟线程实验记录》：对比数字 + pinning 结论 + 是否默认开启的决策与理由。

**明确不做**：CPU 池不改虚拟线程（CPU 密集任务挂虚拟线程毫无收益，还占载体）；
不用 JDK21 新语法重构既有代码；不改任何业务逻辑；旧 JDK8 压测归档口径不覆盖。

**总开关**：`concurrency.virtual-threads.enabled`（默认 `false`）。**回滚方案**：关掉开关重启即回到平台线程，零残留。

---

## 1. 现状盘点（2026-10-27 实测，全部真实文件与行号）

| # | 线程使用点 | 位置 | 现状 | 任务性质 | 虚拟线程判定 |
|---|---|---|---|---|---|
| T1 | Tomcat 请求处理线程池 | Boot3.2 托管 | 平台线程（默认 200 上限） | IO 密集（等 Redis/DB/MQ） | ✅ 一行配置接入 |
| T2 | `taskChainIoExecutor` | `TaskChainExecutorConfig:28-31` | `newFixedThreadPool(8)` | IO_BOUND 节点（Redis/DB/RPC 查询） | ✅ 主改造点 |
| T3 | `taskChainCpuExecutor` | `TaskChainExecutorConfig:23-26` | `newFixedThreadPool(4)` | CPU_BOUND 节点（评分计算） | ❌ 保持平台线程 |
| T4 | 秒杀消费处理池 | `SeckillPlusOrderListener:74-80` | `ThreadPoolExecutor(4/8, queue 1024, CallerRuns)` | DB/Redis IO（建单事务） | ✅ 改造点（需并发上限保护，见 §3） |
| T5 | 缓存重建池 | `CacheClient:45` | `static final newFixedThreadPool(10)` | 逻辑过期异步重建（查 DB + 写 Redis） | ✅ 改造点（static → 实例注入） |
| T6 | `@Scheduled` 7 个定时任务 | Boot3.2 托管 | 单调度线程 + 平台执行 | 混合 | ✅ 随 T1 开关自动生效（fixedDelay 语义不变，需在实录中说明） |
| T7 | RocketMQ 客户端内部线程 | rocketmq-spring 托管 | 厂商管理 | — | ❌ 不动 |
| T8 | 故障注入工具池 | `JvmController:163` | `newCachedThreadPool` | 演练工具本体 | ❌ 不动 |

**关键背景数字**：DB 连接池为 Boot 默认 Hikari（**max 10**，未显式配置）。T2/T4 的并发 DB 访问上限
今天被"8/8 线程"间接保护着——虚拟线程放开并发后，**Hikari 池会变成新的排队点**，这是本实验
最容易翻车的地方（详见 §4 R-3）。

## 2. 设计决策（改哪里、怎么改、为什么）

### 2.1 配置层（T1 + T6）
```yaml
spring:
  threads:
    virtual:
      enabled: true   # 跟随总开关，同值写入
```
由 `concurrency.virtual-threads.enabled` 经 `@Value` 联动，或直接复制同值——**两处必须一起开关**，
验收清单专列一项。

### 2.2 任务链 IO 池（T2）——`TaskChainExecutorConfig`
```java
@Bean("taskChainIoExecutor")
public Executor taskChainIoExecutor(
        @Value("${concurrency.virtual-threads.enabled:false}") boolean virtualEnabled) {
    // 虚拟线程模式：per-task executor，不排队不池化；平台模式：维持固定 8 线程（历史行为零变化）
    return virtualEnabled
            ? Executors.newVirtualThreadPerTaskExecutor()
            : Executors.newFixedThreadPool(8);
}
```
CPU 池代码一行不动。

### 2.3 秒杀消费池（T4）——`SeckillPlusOrderListener`
虚拟线程模式下用 `Executors.newVirtualThreadPerTaskExecutor()`，**并用 `Semaphore` 保护**：
- 平台模式的保护来自"线程数 4-8 + 队列 1024"；虚拟模式没有队列，必须显式限并发；
- 信号量许可数取 `16`（DB Hikari 同步提到 20，见 §3.1，留余量）；
- 拒绝语义变化说明：平台模式队列满走 CallerRuns（消费线程自己跑），虚拟模式无队列——
  信号量 `acquire` 阻塞在 RocketMQ 消费回调上，等效背压，消费不丢消息（MQ 未 ACK）。
- 实现位置：字段初始化改 `@PostConstruct`（需要 `@Value` 注入开关）。

### 2.4 缓存重建池（T5）——`CacheClient`
`static final` 字段改为实例字段 + 构造注入开关（`CacheClient` 已是 `@Component`）：
```java
private final ExecutorService cacheRebuildExecutor;
public CacheClient(StringRedisTemplate template,
        @Value("${concurrency.virtual-threads.enabled:false}") boolean virtualEnabled) {
    ...
    this.cacheRebuildExecutor = virtualEnabled
            ? Executors.newVirtualThreadPerTaskExecutor()
            : Executors.newFixedThreadPool(10);
}
```
注意：逻辑过期重建是"抢锁者一个线程"语义，虚拟化后并发上限天然受锁保护，无需额外信号量。

### 2.5 Hikari 配套（必须与开关同进同退）
```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 20   # 默认 10；虚拟线程放开并发后 DB 池是瓶颈
```
同样挂同一开关意识：压测对照时三种状态（JDK8 / JDK21 关 / JDK21 开）的 Hikari 配置必须显式记录。

### 2.6 明确不改的
T3（CPU 池）、T7（RocketMQ 内部）、T8（演练工具）、`RedisIdWorker`（单线程语义）、
`CACHE_REBUILD` 锁语义、所有业务代码。

## 3. 执行阶段（小步提交）

### Phase A：代码改造（开关默认关）
- [ ] §2.2 / §2.3 / §2.4 / §2.5 四处改造 + `concurrency.virtual-threads.enabled` 属性；
- [ ] `mvn test`（JDK21）106 全绿——**这证明关态零回归**；
- [ ] Commit: `feat: 虚拟线程接入（默认关，见 docs/VIRTUAL-THREADS.md）`。

### Phase B：VM 开态冒烟
- [ ] 上传 jar，`start-app-jdk21.sh` 附加 `--concurrency.virtual-threads.enabled=true --spring.threads.virtual.enabled=true --spring.datasource.hikari.maximum-pool-size=20`（先不改 yaml，用启动参数验证，可回滚）；
- [ ] health UP + 冒烟三项（详情/L1 命中/单发秒杀三方一致）+ `/actuator/prometheus` 正常；
- [ ] **日志确认虚拟线程生效**：任一请求堆栈出现 `VirtualThread[` 或 `jcmd` dump 中虚拟线程数 > 0。

### Phase C：压测对照（本项目最有含金量的产出）
- [ ] 三列同机同参数 R1（4000/3s，每列前先 prepare）：
  - 列 1：JDK8（已有归档 151.5/154，可不重跑，引用 10-24 复测）；
  - 列 2：JDK21 + 开关关（新跑）；
  - 列 3：JDK21 + 开关开（新跑）；
- [ ] 每列记录：吞吐、Avg/p50/p99、HTTP 错误、四数一致、Lua 成功率；
- [ ] 附加观测：`hikaricp_connections_pending`（Hikari 排队）、`jvm_threads_live`（虚拟线程模式应看到线程数飙升——这是预期不是事故）；
- [ ] R4 口径抽检一轮（命中率应与开关无关）。

### Phase D：pinning 审计（R-1 风险的实证）
- [ ] 压测负载进行中：`jcmd <pid> Thread.dump_to_file=/tmp/td.json -m:true`；
- [ ] 检查 JSON 中虚拟线程是否钉在载体上（`pinned` / `carrier` 字段堆积）；
- [ ] 备用手段：JVM 参数 `-Djdk.tracePinnedThreads=full`（JDK21 可用，有观测开销，勿长期挂）；
- [ ] 重点怀疑对象：redisson 内部锁、hutool、mysql-connector-j 驱动——发现钉死堆栈记入实录。

### Phase E：交付
- [ ] 《虚拟线程实验记录》成文（三列对比表 + pinning 结论 + 默认开关决策）；
- [ ] 默认开关决策二选一并写明理由：
  - 虚拟线程占优 → 默认 `true`，README 更新；
  - 持平或劣化 → 保持默认 `false`，"评估并实测过"本身就是结论（负结果同样有面试价值）；
- [ ] Commit + 合回 main + push。

## 4. 风险登记册

| # | 风险 | 概率 | 应对 |
|---|---|---|---|
| R-1 | **synchronized pinning**：请求路径上 redisson/hutool/驱动内部 `synchronized` 块把虚拟线程钉死在载体上，吞吐反降 | 中 | Phase D 实证；若发生→定位堆栈，改 `ReentrantLock`（仅限自有代码）或该环节退回平台线程 |
| R-2 | **DB 连接池排队**：虚拟线程放开并发后 Hikari(10) 成瓶颈，RT 反升 | 高（几乎必然） | §2.5 同步提池到 20；观察 `hikaricp_connections_pending`，若仍排队→信号量收紧或池再提 |
| R-3 | **无界并发打爆下游**：`newVirtualThreadPerTaskExecutor` 无队列，极端洪峰全量直冲 Redis/DB | 中 | T4 信号量限并发；T2 由 Lua/DB 天然限流；压测验证 |
| R-4 | ThreadLocal 语义（`UserHolder`）在虚拟线程上行为 | 低 | ThreadLocal 在虚拟线程完全支持；`afterCompletion` 清理逻辑不变，冒烟覆盖登录态 |
| R-5 | @Scheduled 行为变化（Boot3.2 虚拟调度器） | 低 | fixedDelay 语义保留；冒烟看 7 个 Job 日志正常 |
| R-6 | 吞吐无提升（瓶颈本在下游） | 中-高 | **这不是失败**：如实记录三列数字，"实测无收益 + 知道为什么"就是完整结论，默认开关保持 false |

## 5. 验收清单

- [ ] 开关关闭：106 测试全绿，VM 行为零变化
- [ ] 开关开启：VM 冒烟全过（含指标端点与 L1 命中）
- [ ] 三列 R1 对照数据归档 `dianping/stress-test/results/`（ANALYSIS 文档）
- [ ] pinning 审计完成并留转储证据
- [ ] Hikari pending 观测记录
- [ ] 《虚拟线程实验记录》成文，含默认开关决策
- [ ] 原 JDK8 / 关态归档口径未被覆盖（新增列，不改旧列）

## 6. 时间盒

- Phase A（代码）：约 40 分钟；Phase B（冒烟）：15 分钟；Phase C（三列压测）：约 40 分钟；Phase D（审计）：15 分钟；Phase E（成文）：20 分钟。
- 一次会话可完成 A+B；C+D+E 建议独立会话（压测需要环境安静）。
