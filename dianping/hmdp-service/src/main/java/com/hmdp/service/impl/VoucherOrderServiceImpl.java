package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.shared.SeckillBugFlags;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillVoucherOrderMessage;
import com.hmdp.entity.VoucherReconcileLog;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherReconcileLogMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.VoucherOrderCreateResult;
import com.hmdp.shared.reconciliation.model.ReconcileBusinessType;
import com.hmdp.shared.reconciliation.model.ReconcileLogType;
import com.hmdp.shared.reconciliation.model.ReconciliationStatus;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import jakarta.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * 优惠券订单服务实现 — 秒杀/普通购买/秒杀Plus的核心编排层。
 *
 * <p>秒杀Plus完整流程（seckillVoucherPlus）：
 * <pre>{@code
 * 用户请求 seckillVoucherPlus(voucherId)
 *   │
 *   ├─ 1. RedisIdWorker 预生成订单ID（秒级时间戳 + Redis自增序列，无机器位的雪花变体）+ traceId
 *   │
 *   ├─ 2. 执行 seckill_plus.lua（Redis原子操作）
 *   │      ├─ 请求去重（同一用户1秒内只允许一个请求）
 *   │      ├─ 库存检查 → 不足则返回失败
 *   │      ├─ 一人一单校验 → 重复则返回失败
 *   │      └─ 扣减库存 + 写入 Redis trace → 返回成功
 *   │
 *   ├─ 3. 可靠MQ发送（order-create 消息）
 *   │      ├─ 成功 → 创建建单校验任务
 *   │      └─ 失败 → 回滚Redis库存（seckill_plus_rollback.lua）→ 返回错误
 *   │
 *   ├─ 4. 发送延时消息（order-timeout，delayLevel=15 实际 20 分钟后触发，做兜底 Job 的最后备份）
 *   │
 *   └─ 5. 返回 orderId 给用户
 *
 *  【异步】SeckillPlusOrderListener 消费 order-create 消息
 *   │
 *   ├─ 6. createVoucherOrderPlus() → DB入库 + DB库存扣减 + DEDUCT日志
 *   │      ├─ 主键冲突 → DUPLICATE（幂等）
 *   │      ├─ DB库存不足 → OUT_OF_STOCK → 回滚Redis
 *   │      └─ 成功 → CREATED
 *   │
 *   └─ 7. OrderCreateVerifyJob → 校验订单与 DEDUCT 日志是否最终落库
 *
 *  【定时】OrderCreateVerifyJob → 重试校验，必要时回滚 Redis 预扣库存
 * }</pre></p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    /** 等待异步建单落地的超时时间（秒），用于压测对比时阻塞确认 */
    private static final int ORDER_CREATE_CONFIRM_TIMEOUT_SECONDS = 3;

    /** 秒杀优惠券服务（DB 库存扣减） */
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    /** RocketMQ 原生模板（原始 MQ 操作） */
    @Resource
    private RocketMQTemplate rocketMQTemplate;

    /** 可靠 MQ 发布器（封装了消息发送失败时的本地消息表持久化与重试） */
    @Resource
    private RocketMqReliablePublisher rocketMqReliablePublisher;

    /** 分布式 ID 生成器（秒级时间戳 + Redis 自增序列，用于预生成订单 ID） */
    @Resource
    private RedisIdWorker redisIdWorker;

    /** Redis 字符串模板（执行 Lua 脚本、操作 Redis 缓存） */
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /** Redisson 客户端（原始秒杀用分布式锁保护一人一单） */
    @Resource
    private RedissonClient redissonClient;

    /** 对账日志 Mapper（对账模块未启用时为 null，不影响主流程） */
    @Autowired(required = false)
    private VoucherReconcileLogMapper voucherReconcileLogMapper;

    /** 订单创建校验服务（对账兜底模块未启用时为 null，不影响主流程） */
    @Autowired(required = false)
    private OrderCreateVerificationService orderCreateVerificationService;

    /** 回滚失败记录服务（同包，回滚失败时落库 + 告警，批次1 ④） */
    @Resource
    private RollbackFailureService rollbackFailureService;

    /** 原始秒杀Lua脚本（seckill.lua） */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    /** 秒杀Plus Lua脚本（seckill_plus.lua）：包含请求去重 + 库存扣减 + 一人一单 + Redis trace */
    private static final DefaultRedisScript<Long> SECKILL_PLUS_SCRIPT;
    /** 秒杀Plus回滚Lua脚本（seckill_plus_rollback.lua）：恢复库存 + 移除用户标记 */
    private static final DefaultRedisScript<Long> SECKILL_PLUS_ROLLBACK_SCRIPT;
    /** RocketMQ Topic:Tag — 订单创建消息 */
    private static final String SECKILL_PLUS_ORDER_DESTINATION = "seckill-plus-order-topic:order-create";
    /** RocketMQ Topic:Tag — 订单超时延时消息 */
    private static final String SECKILL_PLUS_TIMEOUT_DESTINATION = "seckill-plus-order-topic:order-timeout";
    /** Redis trace 在 Redis 中的 TTL（2天），过期后不再参与建单校验 */
    private static final long SECKILL_TRACE_TTL_SECONDS = 2 * 24 * 60 * 60L;
    /**
     * 死锁埋点用 — 两把静态锁，按 userId 奇偶决定加锁顺序，故意制造锁顺序不一致以触发死锁。
     * 由 {@link com.hmdp.shared.SeckillBugFlags#DEADLOCK} 开关控制是否启用。
     */
    private static final Object BUG_LOCK_A = new Object();
    private static final Object BUG_LOCK_B = new Object();

    /**
     * 类加载时初始化三个 Redis Lua 脚本
     * <p>
     * Lua 脚本在 Redis 中是原子执行的，适合秒杀场景中需要"多步操作要么全成功要么全失败"的场景：
     * <ul>
     *   <li>{@code seckill.lua} — 原始秒杀：一人一单校验 + 库存扣减</li>
     *   <li>{@code seckill_plus.lua} — 秒杀Plus：请求去重 + 一人一单 + 库存扣减 + trace 记录（含幂等保护）</li>
     *   <li>{@code seckill_plus_rollback.lua} — 秒杀Plus 回滚：恢复库存 + 移除用户标记（含幂等保护）</li>
     * </ul>
     * 脚本放在 {@code resources/} 目录下，通过 {@link ClassPathResource} 加载，返回值统一为 {@link Long}。
     * </p>
     */
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);

        SECKILL_PLUS_SCRIPT = new DefaultRedisScript<>();
        SECKILL_PLUS_SCRIPT.setLocation(new ClassPathResource("seckill_plus.lua"));
        SECKILL_PLUS_SCRIPT.setResultType(Long.class);

        SECKILL_PLUS_ROLLBACK_SCRIPT = new DefaultRedisScript<>();
        SECKILL_PLUS_ROLLBACK_SCRIPT.setLocation(new ClassPathResource("seckill_plus_rollback.lua"));
        SECKILL_PLUS_ROLLBACK_SCRIPT.setResultType(Long.class);
    }

    /**
     * 原始秒杀下单（同步建单版本，已废弃）。
     *
     * <p>流程：Lua扣库存 → 同步调用 createVoucherOrder 写DB。
     * 相比秒杀Plus版本缺少MQ异步解耦和对账机制。</p>
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = 0;
        if (result != null) {
            r = result.intValue();
        }
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        VoucherOrder order = new VoucherOrder();
        order.setId(orderId);
        order.setUserId(userId);
        order.setVoucherId(voucherId);
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        proxy.createVoucherOrder(order);
        return Result.ok(orderId);
    }

    /**
     * 秒杀Plus下单 — 异步建单版本（推荐），完整的 Redis → MQ → DB → 建单校验 链路。
     *
     * <p>与原始秒杀的关键区别：
     * <ul>
     *   <li>Redis Lua 脚本增加了请求去重和 trace 记录</li>
     *   <li>通过 RocketMQ 异步建单，解耦 Redis 扣减和 DB 持久化</li>
     *   <li>MQ 发送失败时自动回滚 Redis 库存</li>
     *   <li>发送延时消息用于超时关单</li>
     *   <li>通过校验任务确认订单与 DEDUCT 日志最终一致</li>
     * </ul></p>
     *
     * @param voucherId 秒杀优惠券ID
     * @return 秒杀结果，成功返回 orderId
     */
    @Override
    public Result seckillVoucherPlus(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        String traceId = "trace-" + orderId;

        // 1. 执行Redis Lua脚本，原子完成资格校验 + 库存扣减 + trace记录
        Long result = stringRedisTemplate.execute(
                SECKILL_PLUS_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId),
                traceId,
                String.valueOf(SECKILL_TRACE_TTL_SECONDS),
                String.valueOf(System.currentTimeMillis())
        );
        int r = result == null ? -1 : result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : r == 2 ? "不能重复下单" : r == 3 ? "请求过于频繁，请稍后重试" : "秒杀库存未初始化");
        }

        // ========== 埋点：JVM问题注入（由SeckillBugConfig开关控制） ==========
        if (SeckillBugFlags.CPU_SPIN.get()) {
            // 模拟"热点数据预热"中的CPU密集型计算
            fibonacciWarmup(42);
        }
        if (SeckillBugFlags.CPU_REGEX.get()) {
            // 模拟"用户ID格式校验"中的灾难性回溯正则
            String suspect = userId.toString() + "1111111111111";
            SeckillBugFlags.BACKTRACK_PATTERN.matcher(suspect).matches();
        }
        if (SeckillBugFlags.DEADLOCK.get()) {
            // 模拟"跨券库存一致性校验"中锁顺序不一致
            if (userId % 2 == 0) {
                synchronized (BUG_LOCK_A) {
                    try { Thread.sleep(50); } catch (InterruptedException ignored) {}
                    synchronized (BUG_LOCK_B) { /* 券A→券B的库存校验 */ }
                }
            } else {
                synchronized (BUG_LOCK_B) {
                    try { Thread.sleep(50); } catch (InterruptedException ignored) {}
                    synchronized (BUG_LOCK_A) { /* 券B→券A的库存校验 */ }
                }
            }
        }

        VoucherOrder order = new VoucherOrder();
        order.setId(orderId);
        order.setUserId(userId);
        order.setVoucherId(voucherId);
        order.setTraceId(traceId);

        // ========== 1) 构造消息（失败时尚未发出任何消息，可以安全回滚 Redis） ==========
        SeckillVoucherOrderMessage message;
        String payload;
        try {
            message = buildOrderMessage(order);
            payload = JSONUtil.toJsonStr(message);
        } catch (Exception e) {
            log.error("build seckill plus order message failed, orderId={}", orderId, e);
            rollbackAfterPublishFailure(order);
            return Result.fail("下单人数过多，请重试");
        }

        // ========== 2) 可靠MQ发送：失败时自动持久化到DB等待重试 ==========
        boolean sent;
        try {
            sent = rocketMqReliablePublisher.publish(
                    "SECKILL_ORDER_CREATE",
                    SECKILL_PLUS_ORDER_DESTINATION,
                    payload
            );
        } catch (Exception e) {
            // publish 内部落库兜底也失败（如 DB 不可用）：消息未确认发出，回滚 Redis
            log.error("publish seckill plus order message failed, orderId={}", orderId, e);
            rollbackAfterPublishFailure(order);
            return Result.fail("下单人数过多，请重试");
        }
        if (!sent) {
            // MQ发送失败 → 回滚Redis库存，用户可重试；回滚失败落库告警，不改变用户可重试语义
            rollbackAfterPublishFailure(order);
            return Result.fail("下单人数过多，请重试");
        }

        // ========== 3) 消息已发出：以下步骤 best-effort，任何失败都不再回滚 Redis ==========
        // publish 成功 = 订单消息必然被消费（消费端幂等兜底）。此时若因后续步骤异常回滚 Redis，
        // 会造成"用户被告知失败、订单却已建成" + Redis 库存虚高，所以这里只记录、不回滚。
        // 2. 等待异步建单落地（已注释，压测对比效果）
//             sleepForOrderConfirm();
        // 登记订单创建校验任务（巡检兜底）
        // ---------------------------------------------------------------------
        // 主链路已注释掉 sleepForOrderConfirm() 同步等待，不阻塞用户响应；
        // 这里登记一条 PENDING 任务交由 OrderCreateVerifyJob（每5秒）巡检：
        //   DB有订单 + DB有DEDUCT日志 → SUCCESS（一致）
        //   还没建好（MQ延迟/Lag）  → RETRYING，下次再查
        //   重试耗尽              → FAILED → RecoveryService 主动补偿建单
        //
        // null 检查：@Autowired(required=false) — 对账兜底模块可整体关掉做压测对照；
        // existsVoucherOrder 预查：本地Broker/极低延迟场景下MQ可能已消费完，
        // 订单此时已落库，跳过登记 = 避免白白多一轮 Job 巡检 + DB 查询。
        // ---------------------------------------------------------------------
        try {
            if (orderCreateVerificationService != null && !existsVoucherOrder(orderId)) {
                orderCreateVerificationService.createPendingTask(orderId, userId, voucherId, traceId);
            }
        } catch (Exception e) {
            // 校验任务落库失败：订单消息已在途，不回滚；对账扫描兜底
            log.warn("create order verify task failed, left to reconciliation, orderId={}", orderId, e);
        }

        // 3. 发送延时消息（delayLevel=15，实际 20 分钟）—— 比兜底Job的15分钟门槛晚，
        //    正常情况兜底Job在16分钟左右先关单，延时消息只在兜底失效时生效，做最后备份。
        //    选20min而不是更早的10min档(delayLevel=14)是因为：
        //    RocketMQ没有精确15min的档位(只有14=10min, 15=20min)，早到的档会在Java层15min门槛
        //    下全跳过(白跑)，反而变成冗余消费；选更晚的档让延时消息真正有意义。
        try {
            rocketMqReliablePublisher.publishDelay(
                    RocketMqReliablePublisher.EVENT_SECKILL_ORDER_TIMEOUT,
                    SECKILL_PLUS_TIMEOUT_DESTINATION,
                    payload,
                    RocketMqReliablePublisher.ORDER_TIMEOUT_DELAY_LEVEL
            );
        } catch (Exception e) {
            // 延时消息失败：订单消息已发出，不回滚；正常关单还有 2 分钟兜底 Job
            log.warn("publish timeout delay message failed, left to fallback job, orderId={}", orderId, e);
        }
        return Result.ok(orderId);
    }

    /**
     * 发布失败路径的统一收口 —— 回滚 Redis 预扣，回滚再失败则落账本。
     * <p>只在 order-create 消息确认未发出时调用；消息一旦确认发出，后续异常一律不回滚
     * （见 {@link #seckillVoucherPlus} 步骤 3 的说明）。</p>
     */
    private void rollbackAfterPublishFailure(VoucherOrder order) {
        try {
            rollbackSeckillVoucherPlus(order);
        } catch (Exception rollbackEx) {
            rollbackFailureService.record(RollbackFailureService.SOURCE_MAIN_CHAIN,
                    order, "REDIS_EXECUTE_ERROR", 1, rollbackEx);
        }
    }

    /**
     * 原始秒杀同步建单（已废弃的版本）。
     *
     * <p>在一人一单锁保护下，校验重复购买 → DB库存扣减 → 订单入库。
     * 库存扣减和订单入库在同一事务中，但没有对账追踪。</p>
     */
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        // MyBatis-Plus 3.5 起 count() 返回 Long
        long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("用户已经购买过一次了");
            return;
        }
        boolean success = seckillVoucherService
                .update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存不足");
            return;
        }
        save(voucherOrder);
    }

    /**
     * 秒杀Plus异步建单 — 由 SeckillPlusOrderListener 消费MQ消息后调用。
     *
     * <p>在事务中完成三项操作：
     * <ol>
     *   <li>插入订单（主键冲突 → DUPLICATE，幂等处理）</li>
     *   <li>DB乐观锁扣减库存（库存不足 → 事务回滚，返回 OUT_OF_STOCK）</li>
     *   <li>写入DEDUCT类型对账日志（记录扣减前后的库存快照）</li>
     * </ol></p>
     *
     * <p>调用方根据返回值决定后续动作：
     * <ul>
     *   <li>CREATED → 写入对账追踪，等待旁路校验</li>
     *   <li>DUPLICATE → 幂等忽略（消息重复消费）</li>
     *   <li>OUT_OF_STOCK → 回滚Redis库存，留给对账引擎兜底</li>
     * </ul></p>
     *
     * @param message MQ消息体
     * @return 创建结果
     */
    @Override
    @Transactional
    public VoucherOrderCreateResult createVoucherOrderPlus(SeckillVoucherOrderMessage message) {
        VoucherOrder voucherOrder = new VoucherOrder()
                .setId(message.getOrderId())
                .setUserId(message.getUserId())
                .setVoucherId(message.getVoucherId())
                .setTraceId(message.getTraceId())
                .setStatus(com.hmdp.shared.reconciliation.model.VoucherOrderStatus.UNPAID)
                .setCreateTime(LocalDateTime.now());

        // 1. 先插入订单 — 主键冲突说明是重复消息，幂等忽略
        try {
            save(voucherOrder);
        } catch (DuplicateKeyException e) {
            log.info("duplicate voucher order ignored, orderId={}, userId={}, voucherId={}",
                    voucherOrder.getId(), voucherOrder.getUserId(), voucherOrder.getVoucherId());
            return VoucherOrderCreateResult.DUPLICATE;
        }

        // 2. DB乐观锁扣库存 —— 与 Redis Lua 脚本形成双扣兜底：Redis 挡高并发洪水，DB 做持久层最终一致
        Integer beforeQty = loadDbStock(voucherOrder.getVoucherId());
        boolean success = seckillVoucherService
                .update()                                            // 开启 UpdateWrapper 链式构建
                .setSql("stock=stock-1")                            // 原生 SQL 表达式，让 DB 原子执行自减，避免 JVM 读写竞态
                .eq("voucher_id", voucherOrder.getVoucherId())      // 限定目标券
                .gt("stock", 0)                                     // WHERE stock > 0 —— 乐观锁式防超卖，库存耗尽时 update 影响行数为 0（非异常）
                .update();
        if (!success) {
            // DB库存不足 → 标记事务回滚（订单插入也会回滚）
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            log.error("db stock not enough when creating seckill plus order, orderId={}", voucherOrder.getId());
            return VoucherOrderCreateResult.OUT_OF_STOCK;
        }

        // 3. 写入DEDUCT对账日志 — 记录DB层的库存变更快照
        saveDeductReconcileLog(message, voucherOrder, beforeQty);

        // ========== 埋点：JVM问题注入（由SeckillBugConfig开关控制） ==========
        if (SeckillBugFlags.MEMORY_LEAK.get()) {
            // 模拟"订单本地缓存加速"，但无淘汰策略
            SeckillBugFlags.LEAK_CACHE.put(voucherOrder.getId(), voucherOrder);
        }
        if (SeckillBugFlags.GC_ALLOC.get()) {
            // 模拟"生成订单报表"中的大对象分配
            byte[] reportBuffer = new byte[5 * 1024 * 1024]; // 5MB
            for (int i = 0; i < reportBuffer.length; i += 4096) {
                reportBuffer[i] = (byte) (i % 256);
            }
        }

        return VoucherOrderCreateResult.CREATED;
    }

    /**
     * 回滚秒杀Plus订单 — 恢复Redis中已扣减的库存。
     *
     * <p>触发场景：
     * <ul>
     *   <li>MQ消息发送失败</li>
     *   <li>DB库存不足（OUT_OF_STOCK）</li>
     *   <li>其他需要回退Redis库存的异常</li>
     * </ul></p>
     *
     * <p>执行 seckill_plus_rollback.lua 恢复库存并在Redis中记录RESTORE日志，
     * 同时在DB中写入RESTORE对账日志。</p>
     */
    @Override
    public void rollbackSeckillVoucherPlus(VoucherOrder voucherOrder) {
        executeGovernanceRollback(voucherOrder);
        saveRestoreReconcileLog(voucherOrder, ReconcileBusinessType.FAIL, "rollback triggered");
    }

    /**
     * 判断订单是否已存在（用于超时关单前的校验）。
     */
    @Override
    public boolean existsVoucherOrder(Long orderId) {
        return getBaseMapper().selectById(orderId) != null;
    }

    /** 递归斐波那契 — CPU密集型，用于模拟"热点数据预热" */
    private static long fibonacciWarmup(int n) {
        if (n <= 1) {
            return n;
        }
        return fibonacciWarmup(n - 1) + fibonacciWarmup(n - 2);
    }

    void sleepForOrderConfirm() {
        try {
            TimeUnit.SECONDS.sleep(ORDER_CREATE_CONFIRM_TIMEOUT_SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 构建MQ消息体 */
    private SeckillVoucherOrderMessage buildOrderMessage(VoucherOrder order) {
        SeckillVoucherOrderMessage message = new SeckillVoucherOrderMessage();
        message.setOrderId(order.getId());
        message.setUserId(order.getUserId());
        message.setVoucherId(order.getVoucherId());
        message.setTraceId(order.getTraceId());
        return message;
    }

    /**
     * 执行Redis回滚 — 调用 seckill_plus_rollback.lua 恢复库存。
     *
     * <p>Lua脚本内部有幂等保护：如果该traceId已经RESTORE过，不会重复回滚。</p>
     */
    private void executeGovernanceRollback(VoucherOrder voucherOrder) {
        stringRedisTemplate.execute(
                SECKILL_PLUS_ROLLBACK_SCRIPT,
                Collections.emptyList(),
                voucherOrder.getVoucherId().toString(),
                voucherOrder.getUserId().toString(),
                String.valueOf(voucherOrder.getId()),
                voucherOrder.getTraceId() == null ? "trace-" + voucherOrder.getId() : voucherOrder.getTraceId(),
                String.valueOf(SECKILL_TRACE_TTL_SECONDS),
                String.valueOf(System.currentTimeMillis())
        );
    }

    /** 读取DB当前库存（用于对账日志记录 beforeQty） */
    private Integer loadDbStock(Long voucherId) {
        com.hmdp.entity.SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        return voucher == null ? null : voucher.getStock();
    }

    /** 写入DEDUCT类型对账日志 — 记录DB库存扣减快照 */
    private void saveDeductReconcileLog(SeckillVoucherOrderMessage message, VoucherOrder order, Integer beforeQty) {
        if (voucherReconcileLogMapper == null) {
            return;
        }
        int afterQty = beforeQty == null ? -1 : beforeQty - 1;
        VoucherReconcileLog log = new VoucherReconcileLog()
                .setOrderId(order.getId())
                .setUserId(order.getUserId())
                .setVoucherId(order.getVoucherId())
                .setTraceId(message.getTraceId())
                .setBeforeQty(beforeQty)
                .setChangeQty(1)
                .setAfterQty(afterQty)
                .setLogType(ReconcileLogType.DEDUCT)
                .setBusinessType(ReconcileBusinessType.SUCCESS)
                .setReconciliationStatus(ReconciliationStatus.PENDING)
                .setDetail("db order created and stock deducted");
        voucherReconcileLogMapper.insert(log);
    }

    /** 写入RESTORE类型对账日志 — 记录库存回补 */
    private void saveRestoreReconcileLog(VoucherOrder order, ReconcileBusinessType businessType, String detail) {
        if (voucherReconcileLogMapper == null) {
            return;
        }
        VoucherReconcileLog log = new VoucherReconcileLog()
                .setOrderId(order.getId())
                .setUserId(order.getUserId())
                .setVoucherId(order.getVoucherId())
                .setTraceId(order.getTraceId() == null ? "trace-" + order.getId() : order.getTraceId())
                .setChangeQty(1)
                .setLogType(ReconcileLogType.RESTORE)
                .setBusinessType(businessType)
                .setReconciliationStatus(ReconciliationStatus.PENDING)
                .setDetail(detail);
        voucherReconcileLogMapper.insert(log);
    }

    /**
     * 处理优惠券订单（Redisson分布式锁版本，原始秒杀用）。
     *
     * <p>通过分布式锁保证同一用户不会并发创建订单，
     * 锁内通过Spring AOP代理调用 @Transactional 方法。</p>
     */
    public void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;
}