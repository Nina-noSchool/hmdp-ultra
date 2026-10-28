package com.hmdp.service.impl;

import com.hmdp.entity.MqEventRecord;
import com.hmdp.mapper.MqEventRecordMapper;
import com.hmdp.metrics.MetricsRecorder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * RocketMQ 可靠消息发布器 — 同步发送 + 失败DB兜底。
 *
 * <p>可靠性保证：
 * <ul>
 *   <li>正常路径：syncSend 同步发送到 RocketMQ Broker</li>
 *   <li>失败路径：消息持久化到 tb_mq_event_record 表，由 MqEventRetryJob 定时重试</li>
 * </ul></p>
 *
 * <p>使用场景（秒杀Plus中）：
 * <ol>
 *   <li>发送 order-create 消息 → SeckillPlusOrderListener 异步建单</li>
 *   <li>发送 order-timeout 延时消息 → SeckillOrderTimeoutListener 超时关单</li>
 * </ol></p>
 *
 * <p>注意：publish 返回 false 仅表示MQ发送失败且已DB兜底，调用方应自行处理业务回滚逻辑。</p>
 */
@Component
@Slf4j
public class RocketMqReliablePublisher {

    /** 超时关单事件类型（VoucherOrderServiceImpl 发送、MqEventRetryJob 重发时共用） */
    public static final String EVENT_SECKILL_ORDER_TIMEOUT = "SECKILL_ORDER_TIMEOUT";
    /** 超时关单延时级别：RocketMQ 第 15 档 = 20 分钟（档位说明见 publishDelay） */
    public static final int ORDER_TIMEOUT_DELAY_LEVEL = 15;

    private final RocketMQTemplate rocketMQTemplate;
    private final MqEventRecordMapper mqEventRecordMapper;
    private final MetricsRecorder metricsRecorder;

    public RocketMqReliablePublisher(RocketMQTemplate rocketMQTemplate,
                                     MqEventRecordMapper mqEventRecordMapper,
                                     MetricsRecorder metricsRecorder) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.mqEventRecordMapper = mqEventRecordMapper;
        this.metricsRecorder = metricsRecorder;
    }

    /**
     * 同步发送可靠消息 — 失败时自动持久化到DB等待重试。
     *
     * @param eventType   事件类型标识（如 SECKILL_ORDER_CREATE）
     * @param destination RocketMQ topic:tag
     * @param payload     消息体JSON
     * @return true=发送成功，false=发送失败已DB兜底
     */
    public boolean publish(String eventType, String destination, String payload) {
        return doPublish(eventType, destination, payload, true);
    }

    /**
     * 重试场景专用：失败不重复落库。
     *
     * <p>MqEventRetryJob 重发的记录本身就来自 tb_mq_event_record，
     * 若重发失败再走 persistFailure 会为同一条消息插入一条新记录 ——
     * Broker 故障期间记录数会随每轮重试成倍增长（重试风暴放大表体积）。
     * 因此重试路径失败只返回 false，由调用方（Job）维护原记录的状态与计数。</p>
     *
     * @return true=发送成功；false=发送失败且不落库
     */
    public boolean resend(String eventType, String destination, String payload) {
        return doPublish(eventType, destination, payload, false);
    }

    /**
     * 同步发送 RocketMQ 消息的内部公共实现 —— 供 {@link #publish} / {@link #resend} 复用。
     *
     * <p>始终走 syncSend 保证调用方能立刻拿到发送结果，
     * 失败兜底策略由 persistOnFailure 决定：
     * <ul>
     *     <li>{@code true} —— 首发生命周期，失败落库（persistFailure）由定时任务补偿</li>
     *     <li>{@code false} —— 重试生命周期，失败不落库（避免重试风暴膨胀 tb_mq_event_record）</li>
     * </ul></p>
     *
     * @param eventType        事件类型标识（如 SECKILL_ORDER_CREATE），落库时用于分类/查询
     * @param destination      RocketMQ 目标 topic（可带 tag，格式 topic:tag）
     * @param payload          消息体 JSON 字符串
     * @param persistOnFailure 发送失败时是否持久化到 DB 等待后续补偿
     * @return true=发送成功；false=发送失败
     */
    private boolean doPublish(String eventType, String destination, String payload, boolean persistOnFailure) {
        try {
            rocketMQTemplate.syncSend(destination, MessageBuilder.withPayload(payload).build());
            return true;
        } catch (Exception e) {
            log.error("rocketmq publish failed, destination={}, persistOnFailure={}", destination, persistOnFailure, e);
            if (persistOnFailure) {
                persistFailure(eventType, destination, payload, e);
            }
            return false;
        }
    }

    /**
     * 发送延时消息 — 请求路径异步化版本（2026-10-28）。
     *
     * <p>零语义变化契约：发送时机（下单时刻）、destination、延时级别、
     * 失败兜底（persistFailure 落 tb_mq_event_record → MqEventRetryJob 重发）
     * 全部与原 syncSend 版本一致；唯一变化是请求线程不再阻塞等 Broker ACK ——
     * 20 分钟后才触发的备份消息没有理由占用下单 RTT（压测实证：请求路径两跳
     * 同步 MQ 是虚拟线程模式下管线内排队的重要一环）。</p>
     *
     * <p>返回值语义变化：true 从"Broker 已确认"变为"已提交到客户端发送队列"。
     * 调用方（seckillVoucherPlus 第 4 步）本就忽略返回值、失败不影响主流程
     * （延时消息只是兜底 Job 的最后备份），因此无语义影响。
     * 异步失败的落库兜底在 {@link SendCallback#onException} 中执行。</p>
     *
     * <p>延时级别（RocketMQ默认）：
     * 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     * 对应levels：1=1s, 2=5s, ..., 14=10m, 15=20m（实际14≈10分钟常见配置）</p>
     *
     * @param eventType   事件类型
     * @param destination RocketMQ topic:tag
     * @param payload     消息体JSON
     * @param delayLevel  延时级别（1-18）
     * @return true=已提交发送；false=提交阶段失败（已DB兜底）
     */
    public boolean publishDelay(String eventType, String destination, String payload, int delayLevel) {
        try {
            rocketMQTemplate.asyncSend(destination,
                    MessageBuilder.withPayload(payload)
                            .setHeader("DELAY", delayLevel)
                            .build(),
                    new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            // 备份消息发送成功，无需处理
                        }

                        @Override
                        public void onException(Throwable e) {
                            log.error("rocketmq delayed async send failed, destination={}, delayLevel={}",
                                    destination, delayLevel, e);
                            persistFailure(eventType, destination, payload, e);
                        }
                    });
            return true;
        } catch (Exception e) {
            log.error("rocketmq delayed async submit failed, destination={}, delayLevel={}",
                    destination, delayLevel, e);
            persistFailure(eventType, destination, payload, e);
            return false;
        }
    }

    /**
     * 延时消息重发（重试场景专用）：失败不重复落库，状态由原记录承载，
     * 且必须保留 delayLevel —— 普通 syncSend 会立刻投递，丢掉延时语义。
     *
     * <p>刻意保持 syncSend：重试路径是低频后台行为（MqEventRetryJob 每 30s），
     * 阻塞无碍；且 Job 依赖同步返回值维护原记录状态 —— 异步化会让
     * "返回 true 但实际失败"的窗口丢失重试机会，破坏重试语义。</p>
     *
     * @return true=发送成功；false=发送失败且不落库
     */
    public boolean resendDelay(String eventType, String destination, String payload, int delayLevel) {
        return doPublishDelay(eventType, destination, payload, delayLevel, false);
    }

    private boolean doPublishDelay(String eventType, String destination, String payload,
                                   int delayLevel, boolean persistOnFailure) {
        try {
            rocketMQTemplate.syncSend(destination,
                    MessageBuilder.withPayload(payload)
                            .setHeader("DELAY", delayLevel)
                            .build());
            return true;
        } catch (Exception e) {
            log.error("rocketmq delayed publish failed, destination={}, delayLevel={}, persistOnFailure={}",
                    destination, delayLevel, persistOnFailure, e);
            if (persistOnFailure) {
                persistFailure(eventType, destination, payload, e);
            }
            return false;
        }
    }

    /**
     * 持久化发送失败的消息到DB — MqEventRetryJob 定时捞取重试。
     *
     * <p>初始重试时间设为30秒后，retryCount=0，
     * 由 MqEventRetryJob 按固定 30 秒间隔重发（不做指数退避）。</p>
     */
    private void persistFailure(String eventType, String destination, String payload, Throwable e) {
        metricsRecorder.incMqEventPersisted();
        MqEventRecord record = new MqEventRecord()
                .setEventType(eventType)
                .setDestination(destination)
                .setPayload(payload)
                .setEventStatus("PENDING_RETRY")
                .setRetryCount(0)
                .setLastError(e.getMessage())
                .setNextRetryTime(LocalDateTime.now().plusSeconds(30))
                .setCreateTime(LocalDateTime.now())
                .setUpdateTime(LocalDateTime.now());
        mqEventRecordMapper.insert(record);
    }
}