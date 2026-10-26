# 升级迁移方案：JDK 8 → 21 + Spring Boot 2.3 → 3.2（副本实验，方案 B）

> **文档性质**：执行规格书——给执行者（AI 或人）的分步指令，每步有明确的完成标志和回滚点。
> **生命周期**：现在以私有文档存在于主仓库根目录；创建迁移分支时移入 `docs/MIGRATION-JDK21.md`
> 作为分支首个 commit；按方案 B，迁移验证通过后随分支合回 main 并成为 GitHub 展示的一部分。
> **铁律**：原 main 分支全程冻结；本方案只允许在 `feat/jdk21-migration` worktree 内动手。

---

## 0. 目标与边界

**成功的定义（缺一不可）**：
1. 迁移分支上全量单测 106 个全绿（与原版同一套验收网）；
2. 副本应用在 VM（JDK 21 运行时）启动、health UP、冒烟通过（详情页 200 + 单发秒杀成功 + 三方一致）；
3. 一份《迁移实录》记录每个坑与版本决策（这是面试资产，与代码同等重要）。

**明确不做**：不使用 JDK21 新语法重构业务代码（record/virtual threads 只做"可选附加实验"）；
不改业务逻辑；不动原 main 的任何文件；不删原 JDK8 的压测归档口径。

**回滚方案**：分支不合即无影响；worktree 目录可随时整删；VM 上副本应用用独立端口/独立日志，与原应用并行或单独停止。

## 1. 现状盘点（实测于 2026-10-24）

### 1.1 版本矩阵（父 pom properties，真实值）

| 组件 | 当前 | 目标 | 策略 |
|---|---|---|---|
| spring-boot-starter-parent | 2.3.12.RELEASE | **3.2.x**（3.2 是 Boot3 的 TLS 长支持线） | parent 直接升 |
| java.version | 1.8 | **21** | properties + compiler release |
| lombok | 1.18.36 | 不动（1.18.30+ 已支持 JDK21） | ✅ 零成本 |
| mybatis-plus-boot-starter | 3.4.3 | **mybatis-plus-spring-boot3-starter 3.5.5+**（artifact 改名） | 改 artifactId + 版本 |
| hutool-all | 5.7.17 | 5.8.x（非必须，但 5.8 对新 JDK 更稳） | 小版本升级 |
| mysql-connector-java | 8.0.28 | **com.mysql:mysql-connector-j**（Boot3 托管，artifact 改名） | 删显式版本交给 BOM |
| spring-data-redis / lettuce | 显式钉 2.6.2 / 6.1.6 | **删除这两个钉版**，交 Boot3 BOM 管理（3.2.x/6.3.x） | ⚠️ 当时钉版本是为了绕 lettuce 版本坑，Boot3 下由 BOM 接管 |
| redisson | 3.13.6 | **3.27+**（旧版在 Spring Data 3 下 RedissonSpringCacheManager 等会崩） | 升级 + 回归分布式锁用例 |
| rocketmq-spring-boot-starter | 2.1.1 | **查证项 R-1**（见风险登记册） | 最高风险，先查证再动手 |
| snakeyaml | 1.x（Boot 托管） | 2.x 随 Boot3 | API 变更见 §4.3 |
| caffeine / micrometer | Boot 托管 | 随 BOM | 计数器命名不变，零成本 |

### 1.2 javax→jakarta 改造面（实测统计）

- `javax.annotation.Resource`：**52 个文件** → `jakarta.annotation.Resource`
- `javax.servlet.http.*`（Session/Request/Response）：**11 个文件**（拦截器/MvcConfig/UserController 等）→ `jakarta.servlet.http.*`
- `javax.annotation.Generated`：1 个文件（ShopServiceImpl，无用注解可直接删）
- 合计 **64 处文件级修改**，纯机械，可用正则批量替换 + 编译验证
- 项目源文件总数 297，改动面约 21%

### 1.3 配置文件变更点（Boot3 属性改名，漏一个就静默失效）

| 属性 | 2.3 | 3.2 |
|---|---|---|
| Redis 连接 | `spring.redis.*` | **`spring.data.redis.*`**（application.yaml 整段搬家） |
| Prometheus 导出 | `management.metrics.export.prometheus.*` | **`management.prometheus.metrics.export.*`** |
| 其余（server.tomcat.threads、rocketmq.*、seckill.*、cache.local、flash-pay） | — | 不变 |

⚠️ `management.metrics.export.prometheus.enabled=true` 若忘改，指标端点会静默消失——验收清单里有专门一项。

## 2. 执行阶段（小步提交，每步一个可回滚点）

### Phase 0：环境与副本（约 15 分钟）
- [ ] 本机安装 JDK 21（Temurin 21，winget 或 zip 到 `C:\Users\ZZCstupid\.jdks\`，与 corretto-8 并存）；
- [ ] `git branch feat/jdk21-migration` + `git worktree add D:\computer\ppppp\hmdp-mq-jdk21 feat/jdk21-migration`；
- [ ] 把本方案文档移入副本 `docs/MIGRATION-JDK21.md`，作为分支第一个 commit；
- [ ] 验证：原仓库 `git status` 干净、main 未动。

### Phase 1：只改 pom，不改代码（先看编译器报多少）
- [ ] 按 §1.1 表改父 pom（parent/java.version/各依赖版本与 artifactId）；
- [ ] 删除 spring-data-redis、lettuce 的显式 `<version>`；
- [ ] 本机 JAVA_HOME 切 21，`mvn clean compile` —— **预期大量报错，这一步的目的就是拿到完整错误清单**；
- [ ] Commit: `build: pom 升级至 Boot 3.2 / JDK 21（编译错误清单见迁移实录）`。

### Phase 2：javax→jakarta 机械替换（约 30 分钟）
- [ ] 64 处 import 批量替换（`javax.annotation.Resource`→`jakarta.annotation.Resource`；
      `javax.servlet`→`jakarta.servlet`；删 `javax.annotation.Generated`）；
- [ ] `mvn compile` 直到只剩"非 import 类"错误；
- [ ] Commit: `refactor: javax → jakarta（64 文件）`。

### Phase 3：依赖兼容修复（不确定项集中区，预计占一半总工时）
- [ ] **R-1 rocketmq-spring**（先查证再动手，见 §5）；
- [ ] snakeyaml 2：`HmDianPingApplicationYamlTest` 的 `new Yaml()` → `new Yaml(new LoaderOptions())`；
- [ ] mybatis-plus：分页/乐观锁拦截器配置类按 3.5.x API 核对（`MybatisPlusInterceptor` 写法 3.4→3.5 兼容，重点核对 `PaginationInnerInterceptor`）；
- [ ] redisson：分布式锁（`RLock`）与看门狗用法核对，启动配置类如有 `RedissonSpringCacheManager` 相关需按新 API；
- [ ] Spring 6 行为差异巡检：`@ConfigurationProperties` 构造绑定未用（无害）、`PathPatternParser` 默认路由（拦截器 `/**` 语义不变，应无感）、`RestTemplate` 如有使用需确认无 javax 依赖泄漏；
- [ ] 循环跑到 `mvn test` 全绿（106 个）；
- [ ] Commit: `fix: 依赖兼容修复（详见迁移实录）`。

### Phase 4：配置搬家
- [ ] application.yaml 按 §1.3 搬 `spring.data.redis.*`、`management.prometheus.metrics.export.*`；
- [ ] `HmDianPingApplicationYamlTest` 断言同步更新（它断言 exclude 与 send-message-timeout，路径不受影响则不改）；
- [ ] Commit: `config: Boot3 属性迁移（redis→data.redis，prometheus 导出路径）`。

### Phase 5：验收（本机）
- [ ] `mvn test` 全绿，记录数字；
- [ ] 本机起副本应用（连 VM 中间件），health UP + `/shop/1` 200；
- [ ] Commit 打 tag `jdk21-migration-rc1`。

### Phase 6：VM 部署验证
- [ ] VM 安装 JDK21：`sudo dnf install java-21-openjdk`（Rocky9 官方源有）；
- [ ] 写 `start-app-jdk21.sh`：复制 start-app.sh，JAVA 指向 21，**加 `--server.port=8082`**（与原 JDK8 应用并行，互不干扰）；
- [ ] 上传副本 jar → 启动 → health（8082）→ 冒烟三项（详情页 200 / 单发秒杀成功 / 三方一致）；
- [ ] 注意：两个应用连同一个 Redis/MySQL，冒烟秒杀会真实扣库存——先跑 prepare 再单发，测完恢复；
- [ ] Commit: `test: VM JDK21 并行部署验证通过`。

### Phase 7：可选附加实验（做不做不影响迁移完成）
- [ ] 虚拟线程开关：`spring.threads.virtual.enabled=true` + R4 口径再测一轮（看命中率与 Redis 读流量有没有被 tomcat 虚拟线程改变）；
- [ ] R1 复测：JDK8 jar vs JDK21 jar 同机同参数（4000/3s），拿到真实对比数字（这是计划 B 最有含金量的副产品）。

### Phase 8：交付（方案 B 生效）
- [ ] 《迁移实录》成文（版本矩阵最终态、每个坑的现象/定位/解法、JDK8 vs 21 对比数据）；
- [ ] README 增加"技术演进"段（方案 B 合并后展示）；
- [ ] 合回 main：`git switch main && git merge --no-ff feat/jdk21-migration`（保留分支拓扑）→ push；
- [ ] 简历项目描述技术栈行同步（此时才改简历，之前不动）。

## 3. 验收清单（全部勾选才算迁移成功）

- [ ] 迁移分支 106 单测全绿
- [ ] VM JDK21 副本 health UP（独立端口）
- [ ] 冒烟：详情页 200 / 单发秒杀成功 / Redis-DB-订单三方一致
- [ ] `/actuator/prometheus` 指标端点有数据（验证 §1.3 属性改名没漏）
- [ ] 缓存链路验证：`/shop/1` 二次请求走 L1（hits 增长）
- [ ] MQ 链路验证：秒杀订单落库 + 本地消息表无 FAILED 残留
- [ ] 《迁移实录》完成并随分支提交
- [ ] 原 main 与原 VM 应用未受任何影响（health 正常、git 无改动）

## 4. 风险登记册

| # | 风险 | 概率 | 应对 |
|---|---|---|---|
| R-1 | **rocketmq-spring 对 Boot3 的官方支持**：2.2.3 及以前面向 Boot2（javax）；Boot3 需要 jakarta 版本。执行时先查 Maven Central 的 rocketmq-spring 最新版与官方兼容声明 | 中-高 | 三级预案：① 若有官方 Boot3 版本→直接升；② 若只有半兼容→升到最高版后自行处理 jakarta 冲突（该 starter 的 jakarta 化面小，可 fork 一两个类）；③ 最坏情况→去掉 starter，直接用 rocketmq-client + 自写 Producer/Consumer 包装（本项目 MQ 用面窄：syncSend + 3 个 Listener + 延时消息，包装工作量可控） |
| R-2 | redisson 3.13→3.27 跨度大，API 小改 | 低 | 升级后跑锁相关用例即可覆盖 |
| R-3 | mybatis-plus 3.4→3.5 分页/字段类型处理差异 | 中 | 106 测试覆盖面足够；另在 VM 冒烟覆盖真实 SQL 路径 |
| R-4 | Spring6 默认 PathPattern 路由与拦截器语义 | 低 | `/**` 与排除路径语义不变，验收冒烟覆盖 |
| R-5 | 配置属性改名遗漏（静默失效） | 中 | §3 验收清单专列指标端点一项；yaml 改动全部进 diff review |
| R-6 | JDK21 下旧 CGLIB/反射限制（Spring6 自带 JDK 兼容层，一般无感） | 低 | 单测 + 冒烟兜底 |

## 5. 时间盒与执行节奏

- 预计总工时：**一个晚上（3-4 小时）**；Phase 3 的 R-1 查证与预案可能吃掉一半；
- 建议节奏：一次会话做 Phase 0-2（机械部分），第二次会话做 Phase 3-5（联调），第三次做 Phase 6-8（VM + 交付）；
- 每次会话结束必须停在"测试可运行/commit 已打"的状态，不留在半编译态。

## 6. 与原项目的隔离保证

- worktree 目录独立：`D:\computer\ppppp\hmdp-mq-jdk21`，不影响 `D:\computer\ppppp\hmdp-mq`；
- 原仓库在迁移期间只接受两种操作：`git fetch`、读文件；任何修复继续走 main 的正常流程（迁移分支定期 rebase/merge main 保持同步）；
- VM 副本应用端口 8082 + 独立日志文件，与原应用（8081）并行；
- GitHub 默认分支在 Phase 8 之前保持 main——展示面在合并前不发生变化。

---

## 7. 迁移实录（2026-10-27 执行完成）

### 7.1 实际执行轨迹

| Phase | Commit | 实际结果 |
|---|---|---|
| 0 | `9a816c8` | 分支 + worktree + 本规格书；main 全程未动 ✅ |
| 1 | `5b2aedd` | pom 升级；**R-1 解除**：rocketmq-spring 2.3.1（2024-07）官方支持 Spring Boot 3.x（jakarta）✅ |
| 2 | `8b4d00c` | javax→jakarta 54 文件（含 1 处全限定 `@javax.annotation.Resource` 漏网）；全 10 模块 release 21 编译通过 ✅ |
| 3 | `5e92a75` | 测试适配；**JDK21 运行时全量 106 测试全绿**（27/47/10/2/1/6/10/3，与 JDK8 基线逐模块一致）✅ |
| 4 | `d13ef28` | 配置属性迁移（yaml + RedissonConfig `@Value`）✅ |
| 5 | — | 本机起应用跳过（VM 中间件端口不对局域网开放），并入 Phase 6 直接 VM 验证 |
| 6 | （本 commit） | VM 并行部署 + 冒烟全过，见 §7.3 ✅ |

### 7.2 实际踩坑（与预想差异）

1. **R-1 从"最高风险"降为"零风险"**：rocketmq-spring 2.3.0（2024-02）官方声明 Support SpringBoot 3.x，2.3.1 修补——三级预案一档都没用上。
2. **MP 3.4→3.5 的真实差异只有两类**：① `count()` 返回 `int`→`Long`（主代码 2 处 + 测试 3 处 selectCount 打桩）；② `BaseMapper` 新增 `insert(Collection<T>)` 重载，导致 Mockito 裸 `any()` 编译歧义（指定 DO 类型即解）。
3. **旧 redisson 的传递依赖消失**：redisson 3.13.6 曾传递 `io.reactivex.rxjava2`，UserController 有一个从未使用的死 import 因此暴露——删除（3.27.2 不再携带）。
4. **运行时 JDK 与编译 JDK 要分清**：Mockito 5.7（Boot3.2 托管）的 ByteBuddy 不认识 JDK25 的 class file 69——**测试必须用 JDK21 跑**，用更新 JDK 跑会爆 mock 创建错误（与代码无关）。
5. **snakeyaml 2 的 no-arg 构造器保留**（仅标注 deprecated）：规格书预想的 `new Yaml(new LoaderOptions())` 改造实际不需要。
6. **RedissonConfig 的 `@Value` 用的是属性路径而非 BOM 对象**：`spring.redis.*` → `spring.data.redis.*` 的改名在 Java 代码里也有一份，规格书只列了 yaml——属性改名类风险要 grep 代码。
7. **VM 的 JDK21 一条命令装好**：`dnf install java-21-openjdk`（Rocky9 官方源 21.0.12.1.1）。
8. **冷启动第一发请求必失败是既有现象**（MQ producer 首连超时 / Lua 首调 declined），JDK8 原版同样存在，与迁移无关；本地消息表 30s 重发自愈。

### 7.3 VM 验收证据（2026-10-27，rocky9-dev）

- 并行部署：8081（JDK8 原版）+ 8082（JDK21 副本，`start-app-jdk21.sh`，独立日志 `app-jdk21.log`），39s 双双 health UP；
- 详情页（8082）：3 连发 200，首发 154ms 回源后 L1 命中（`hmdp_cache_local_hits_total` 2.0→3.0）；
- **指标端点有数据**（R-5 验证）：`/actuator/prometheus` 返回 9 个 `hmdp_*` 指标——属性改名无一遗漏；
- 单发秒杀（8082）：订单号返回，三方一致（redis_stock 3999 / redis_orders 1 / db_orders 1 / db_stock 3999）；
- **JDK21 消费者独立验证**：停掉 8081 后单发 → 订单落库，`app-jdk21.log` 出现 "seckill plus order created"——rocketmq-spring 2.3.1 的 Listener 容器在新栈真实工作；
- MQ 健康：`tb_mq_event_record` 仅 SENT，无 FAILED 残留；
- 原应用隔离：8081 全程健康，main 分支零改动。

### 7.4 遗留与可选实验（不阻塞合并）

- Phase 7 附加实验未做：虚拟线程开关（`spring.threads.virtual.enabled=true`）+ JDK8 vs JDK21 同机 R1 对比——后续想做随时可加；
- 简历技术栈行同步（JDK21 / Boot3.2）待项目主人自行更新；
- 8082 副本应用当前仍在 VM 上运行（与 8081 并行），不需要时 `kill $(cat app-jdk21.pid)` 即可。
