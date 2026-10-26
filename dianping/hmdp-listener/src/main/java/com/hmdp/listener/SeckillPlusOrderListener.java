package com.hmdp.listener;

import cn.hutool.json.JSONUtil;
import com.hmdp.shared.SeckillBugFlags;
import com.hmdp.dto.SeckillVoucherOrderMessage;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.metrics.MetricsRecorder;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.VoucherOrderCreateResult;
import com.hmdp.service.impl.OrderCreateRecoveryService;
import com.hmdp.service.impl.RollbackFailureService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import jakarta.annotation.PostConstruct;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 秒杀Plus订单创建监听器 — 消费 order-create 消息，异步完成DB建单。
 *
 * <p>在秒杀Plus流程中的位置（第5步）：
 * <pre>{@code
 *   VoucherOrderServiceImpl.seckillVoucherPlus()
 *     → Redis Lua原子扣库存
 *     → RocketMQ order-create 消息
 *     → 【本Listener】消费 → createVoucherOrderPlus()
 * }</pre></p>
 *
 * <p>并发处理设计：
 * <ul>
 *   <li>批量拉取消息（最多32条），通过线程池并发处理</li>
 *   <li>每条消息独立调用 createVoucherOrderPlus()，结果互不影响</li>
 *   <li>OUT_OF_STOCK 时自动触发 Redis 回滚</li>
 *   <li>DUPLICATE 时幂等忽略（消息重复消费场景）</li>
 *   <li>线程池拒绝策略为 CallerRunsPolicy — 防止消息丢失</li>
 * </ul></p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = "seckill-plus-order-topic",
        consumerGroup = "seckill-plus-order-consumer-group",
        selectorExpression = "order-create"
)
public class SeckillPlusOrderListener implements RocketMQListener<String>, RocketMQPushConsumerLifecycleListener {
    /**
     * 重投次数阈值：达到该值说明监听器此前异常退出过（例如 recovery_task 落库失败触发了
     * Broker 整批重投），此时不再抛给批量监听器，直接在本地写恢复任务。
     * 注意：本链路刻意不依赖 RocketMQ 重试（README 取舍 #2），该值只用于区分
     * "首次投递"与"异常退出后的反复重投"。
     */
    private static final int REDELIVERY_THRESHOLD = 3;

    private final IVoucherOrderService voucherOrderService;
    private final OrderCreateRecoveryService orderCreateRecoveryService;
    private final RollbackFailureService rollbackFailureService;
    private final MetricsRecorder metricsRecorder;

    /** 虚拟线程开关（见 docs/VIRTUAL-THREADS.md）：默认 false 维持平台线程池历史行为 */
    @Value("${concurrency.virtual-threads.enabled:false}")
    private boolean virtualThreadsEnabled;

    /** 订单处理线程池 — 核心线程数=CPU核数，最大=2倍CPU核数，有界队列1024，拒绝策略CallerRuns；
     *  虚拟线程模式下换成 per-task executor，并发上限由 consumePermits 信号量保护 */
    private ExecutorService executor;

    /** 虚拟线程模式的并发上限（保护 DB 连接池；平台模式为 null 不生效） */
    private Semaphore consumePermits;

    @PostConstruct
    void initExecutor() {
        if (virtualThreadsEnabled) {
            this.executor = Executors.newVirtualThreadPerTaskExecutor();
            this.consumePermits = new Semaphore(16);
            log.info("seckill order listener using virtual threads with semaphore permits=16");
        } else {
            this.executor = new ThreadPoolExecutor(
                    Runtime.getRuntime().availableProcessors(),
                    Runtime.getRuntime().availableProcessors() * 2,
                    60, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(1024),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );
        }
    }

    /** 单条消息处理入口（Spring RocketMQ 回调） */
    @Override
    public void onMessage(String message) {
        SeckillVoucherOrderMessage orderMessage = JSONUtil.toBean(message, SeckillVoucherOrderMessage.class);
        processMessage(orderMessage, message, 0);
    }

    /**
     * 处理单条订单创建消息。
     *
     * <p>根据 createVoucherOrderPlus 的返回值分4种情况处理：
     * <ol>
     *   <li>CREATED → 订单创建成功</li>
     *   <li>OUT_OF_STOCK → 回滚Redis库存（DB库存不足，恢复Redis供其他用户抢购）</li>
     *   <li>DUPLICATE → 幂等忽略（消息重复消费）</li>
     *   <li>ROLLBACK_FAILED → 仅记日志，留给对账引擎兜底</li>
     * </ol></p>
     *
     * <p>异常兜底：
     * 当 createVoucherOrderPlus 因为DB连接池耗尽、死锁等基础设施问题抛出异常时，
     * 事务已回滚（订单未落库），必须主动回滚 Redis 库存，否则库存永久泄漏。
     * 回滚失败则写 RESTORE 对账日志，供 SeckillReconcileScanJob 兜底。</p>
     */
    void processMessage(org.apache.rocketmq.common.message.MessageExt msg) {
        String body = new String(msg.getBody(), StandardCharsets.UTF_8);
        SeckillVoucherOrderMessage orderMessage = JSONUtil.toBean(body, SeckillVoucherOrderMessage.class);
        processMessage(orderMessage, body, msg.getReconsumeTimes());
    }

    /**
     * 秒杀下单消息核心处理 —— 消费端真正干活的地方。
     *
     * <p>被上层 {@link #processMessage(MessageExt)} 解码后调用，职责是：
     * ① 先跑故障注入埋点（线上才开，压测模拟 JVM 问题）；
     * ② 调 Service 层建订单；
     * ③ 根据建单结果分流处理：库存不足 → 回滚 Redis / 重复消息 → 幂等跳过 / 异常 → 交给 MQ 或恢复任务。</p>
     *
     * <p>异常处理策略：正常路径下任何异常均被上层 prepareStart 批量监听器捕获，
     * 捕获后写入 recovery_task 表生成恢复任务，由 OrderCreateRecoveryJob 定时兜底补单，
     * Broker 收到 CONSUME_SUCCESS（不触发整批重试，避免雪崩）；
     * 仅当恢复任务落库本身失败时异常外抛、触发整批重投作为兜底。</p>
     *
     * @param orderMessage   反序列化后的秒杀下单消息（含 orderId/userId/voucherId/traceId）
     * @param sourceMessage  原始 JSON 字符串（写入 recovery_task 表用，保留完整原文方便排查）
     * @param reconsumeTimes 当前消息的重投次数（RocketMQ 内部计数器；本链路不依赖 MQ 重试，仅用于区分首次投递与异常退出后的反复重投）
     */
    private void processMessage(SeckillVoucherOrderMessage orderMessage, String sourceMessage, int reconsumeTimes) {
        // 把消息里的四个字段拷进 VoucherOrder 实体 —— 后面 Service 调用和日志都用这个对象
        VoucherOrder order = new VoucherOrder()
                .setId(orderMessage.getOrderId())
                .setUserId(orderMessage.getUserId())
                .setVoucherId(orderMessage.getVoucherId())
                .setTraceId(orderMessage.getTraceId());

        // ========== 故障注入埋点（线上通过 SeckillBugConfig 动态开关） ==========
        // 线程本地泄漏：模拟消息解析时把大 buffer 塞进 ThreadLocal 没清 → 堆内存持续膨胀 → OOM
        if (SeckillBugFlags.THREADLOCAL_LEAK.get()) {
            SeckillBugFlags.LEAK_THREADLOCAL.set(new byte[3 * 1024 * 1024]);
        }
        // 线程爆炸：模拟每条消息起独立 newCachedThreadPool + 60s 长任务 → 线程数飙升 → CPU 飙满
        if (SeckillBugFlags.THREAD_EXPLOSION.get()) {
            java.util.concurrent.ExecutorService auditPool =
                    java.util.concurrent.Executors.newCachedThreadPool();
            auditPool.submit(() -> {
                try { Thread.sleep(60_000); } catch (InterruptedException ignored) {}
            });
        }

        // ========== 核心建单：调 Service 层的 createVoucherOrderPlus ==========
        VoucherOrderCreateResult result;
        try {
            result = voucherOrderService.createVoucherOrderPlus(orderMessage);
        } catch (Exception e) {
            // 异常分两种情况处理（两条路径最终都以 recovery_task 收口，本链路刻意不依赖 MQ 重试）：
            if (reconsumeTimes < REDELIVERY_THRESHOLD) {
                // ① 首次/早期投递：抛给上层 prepareStart 批量监听器，由它统一写入 recovery_task 表
                log.warn("createVoucherOrderPlus failed, will throw for batch recovery handoff, orderId={}, reconsumeTimes={}",
                        order.getId(), reconsumeTimes, e);
                throw e;
            }
            // ② 已达重投阈值 → 说明监听器此前异常退出过（如恢复任务落库失败），这里直接落 recovery_task
            log.error("createVoucherOrderPlus failed after redeliveries, create recovery task, orderId={}, reconsumeTimes={}",
                    order.getId(), reconsumeTimes, e);
            if (orderCreateRecoveryService != null) {
                orderCreateRecoveryService.createRecoveryTask(orderMessage, sourceMessage, reconsumeTimes);
            }
            return;
        }

        // ========== 建单结果分发 ==========

        // 分支 1：DB 扣库存失败（Redis 够但 DB 不够，极端并发下才会出现的库存不一致）
        // → 必须把 Redis 里扣掉的库存 + 一人一单标记 + DEDUCT 日志回滚掉
        //   否则 Redis 库存会被"幽灵单"永久占着，真实用户买不到
        if (result == VoucherOrderCreateResult.OUT_OF_STOCK) {
            // 回滚失败不再向上抛：落 tb_rollback_failure_log + 告警，由对账扫描兜底（批次1 ④）
            try {
                voucherOrderService.rollbackSeckillVoucherPlus(order);
                log.warn("seckill plus order out of stock, rolled back, orderId={}", order.getId());
            } catch (Exception rollbackEx) {
                log.error("seckill plus rollback failed on out-of-stock, orderId={}", order.getId(), rollbackEx);
                rollbackFailureService.record(RollbackFailureService.SOURCE_LISTENER,
                        order, "REDIS_EXECUTE_ERROR", 1, rollbackEx);
            }
            return;
        }
        // 分支 2：ROLLBACK_FAILED —— ⚠️ 预留扩展，当前是死代码
        // 实际回滚失败不走这里，而是上面分支 1 里 rollbackSeckillVoucherPlus() 抛异常
        // 被外层 catch（行 174-178）捕获 → rollbackFailureService.record() 写 tb_rollback_failure_log
        // → SeckillReconcileScanJob 对账引擎扫 DEDUCT 日志 vs DB 订单表做最终兜底
        // 保留此分支是为将来 rollbackSeckillVoucherPlus 改为返回值驱动时启用
        if (result == VoucherOrderCreateResult.ROLLBACK_FAILED) {
            log.error("seckill plus rollback failed, left to reconciliation, orderId={}", order.getId());
            return;
        }
        // 分支 3：返回 DUPLICATE —— 说明同一个 orderId 的消息已经被消费过了
        // （可能是 RocketMQ 重投、也可能是上游发了重复消息）
        // 直接幂等忽略，不报错、不重试，保证消费端安全
        if (result == VoucherOrderCreateResult.DUPLICATE) {
            log.info("duplicate seckill plus order message ignored, orderId={}", order.getId());
            return;
        }
        // 正路径：建单成功，打完日志就完了
        log.info("seckill plus order created, orderId={}", order.getId());
    }

    /**
     * 配置批量并发消费 —— RocketMQ 消费者启动扩展钩子。
     * <p>
     * 覆盖默认行为（单条串行消费），改写为：
     * <ol>
     *   <li><b>批量拉取</b>：每次向 Broker 拉取最多 32 条消息，减少网络交互</li>
     *   <li><b>并发处理</b>：32 条消息丢进自建线程池并行执行 {@link #processMessage}()</li>
     *   <li><b>全部完成才 ACK</b>：用 CompletableFuture 等待所有任务结束，
     *       再返回 {@link ConsumeConcurrentlyStatus#CONSUME_SUCCESS} 批量确认，
     *       保证 Broker 认为"已消费"时所有消息真的处理完毕，不会丢</li>
     * </ol>
     * 线程池拒绝策略为 {@link ThreadPoolExecutor.CallerRunsPolicy}，
     * 线程池满时让消费者线程自己跑任务，形成反压，防止秒杀洪峰打爆下游。
     *
     * <p><b>异常处理策略（刻意取舍，见 README 取舍 #2 / 面经 Q19 追问 1）</b>：
     * 本监听器<b>始终返回 CONSUME_SUCCESS</b>，单条失败由下面的 catch 转为 recovery_task
     * （OrderCreateRecoveryJob 按 10s 间隔重放），主动放弃 MQ 层自动重试——批量语义下
     * 回抛 RECONSUME_LATER 会让整批 32 条重投，DB 故障时形成雪崩式重试。
     * 唯一的例外：recovery_task 落库本身失败（如 DB 完全不可用）时异常会外抛，
     * 触发 MQ 整批重投作为最后兜底。</p>
     *
     * @param consumer RocketMQ Push 消费者实例（连接 Broker 之前调用）
     */
    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        // 懒初始化兜底：生产环境 @PostConstruct 已按开关建池；直 new（单测）未走 PostConstruct，此处补齐为平台池
        if (executor == null) {
            initExecutor();
        }
        // ① 设置批量拉取上限：默认 1，改成 32，一次和 Broker 交互拿更多消息
        consumer.setConsumeMessageBatchMaxSize(32);

        // ② 替换默认的串行监听器为"并发批量监听器"
        consumer.setMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            List<CompletableFuture<Void>> futures = new ArrayList<>(msgs.size());

            // 逐条包装成异步任务，丢进自建线程池并发执行
            for (MessageExt msg : msgs) {
                futures.add(CompletableFuture.runAsync(() -> processMessage(msg), executor));
            }

            // ③ 阻塞等待所有任务完成 —— 用索引循环，方便异常时拿到对应的原始消息
            for (int i = 0; i < futures.size(); i++) {
                try {
                    futures.get(i).get();
                } catch (Exception e) {
                    // 单条消息处理异常 → 生成恢复任务，让 OrderCreateRecoveryJob 定时兜底补单；
                    // 注意：这里不要用 try-catch 吞掉 createRecoveryTask 的异常——落库失败时让它外抛，
                    // 触发 MQ 整批重投，这是"恢复任务也写不进去"时的唯一兜底。
                    MessageExt failedMsg = msgs.get(i);
                    String body = new String(failedMsg.getBody(), StandardCharsets.UTF_8);
                    SeckillVoucherOrderMessage orderMessage = JSONUtil.toBean(body, SeckillVoucherOrderMessage.class);
                    log.error("batch process message failed, create recovery task, orderId={}",
                            orderMessage.getOrderId(), e);
                    metricsRecorder.incConsumeBatchError();
                    if (orderCreateRecoveryService != null) {
                        orderCreateRecoveryService.createRecoveryTask(orderMessage, body, failedMsg.getReconsumeTimes());
                    }
                }
            }

            // ④ 全部处理完才告诉 RocketMQ：这批我消费成功了
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
    }
}