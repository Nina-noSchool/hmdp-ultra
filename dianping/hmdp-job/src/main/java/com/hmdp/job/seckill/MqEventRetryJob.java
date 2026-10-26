package com.hmdp.job.seckill;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.MqEventRecord;
import com.hmdp.mapper.MqEventRecordMapper;
import com.hmdp.metrics.MetricsRecorder;
import com.hmdp.service.impl.RocketMqReliablePublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;

/**
 * MQ事件重试Job — 每30秒捞取 PENDING_RETRY 状态的消息，重新尝试发送。
 *
 * <p>工作流程：
 * <pre>{@code
 *   每30秒 → 查询 PENDING_RETRY 且 next_retry_time <= now 的记录（最多20条）
 *     ├─ 重新发送成功 → event_status = SENT
 *     ├─ 再次失败且未达上限 → retryCount + 1, next_retry_time = now + 30秒
 *     └─ 再次失败且达到上限 → event_status = FAILED（终态）
 * }</pre></p>
 *
 * <p>重试策略：固定30秒间隔，不做指数退避（秒杀场景对延迟敏感）；
 * 上限默认 5 次（seckill.mq-event.max-retry-count 可配），达到上限转 FAILED 终态、
 * 不再被扫描，等待人工排查（与恢复任务转 MANUAL_REVIEW 同一思路，批次1 ⑤）。</p>
 */
@Component
@Slf4j
public class MqEventRetryJob {

    /** 查询 / 更新 MQ 事件记录表 —— 扫描待重试消息、回写发送结果 */
    @Resource
    private MqEventRecordMapper mqEventRecordMapper;

    /** 可靠消息发布器 —— 将待重试记录重新发送到 RocketMQ */
    @Resource
    private RocketMqReliablePublisher rocketMqReliablePublisher;

    /** 业务指标登记处（批次2 ③）：重试耗尽事件计数 */
    @Resource
    private MetricsRecorder metricsRecorder;

    /** 最大重试次数：达到后转 FAILED 终态等待人工处理（默认 5，可配置） */
    @Value("${seckill.mq-event.max-retry-count:5}")
    private int maxRetryCount = 5;

    /** 待重试状态 */
    static final String STATUS_PENDING_RETRY = "PENDING_RETRY";

    /** 重试耗尽终态（等待人工介入，不再被 Job 扫描） */
    static final String STATUS_FAILED = "FAILED";

    /**
     * 扫描并重试 MQ 发送失败的事件
     * <p>
     * cron = "0/30 * * * * ?" 含义：从第 0 秒开始，每 30 秒触发一次（每分钟的 :00 和 :30）。
     * 单轮最多处理 20 条，避免某轮积压太多消息导致 MQ 压力骤增。
     * </p>
     * <p>
     * 处理逻辑（每条记录独立 try-catch，互不影响）：
     * <pre>{@code
     * 发送成功 → event_status=SENT，不再重试
     * 发送失败 → retryCount+1，next_retry_time 推迟 30 秒，等待下轮
     * 抛异常   → 打 WARN 日志后跳过，防止一条坏消息阻断整个 Job
     * }</pre></p>
     */
    @Scheduled(cron = "0/30 * * * * ?")
    public void run() {
        // 防御性空值检查 —— 某些启动阶段 Bean 可能尚未注入完成
        if (mqEventRecordMapper == null || rocketMqReliablePublisher == null) {
            return;
        }
        // 构建查询条件
        QueryWrapper<MqEventRecord> wrapper = new QueryWrapper<>();
        // 查询 PENDING_RETRY 状态且 next_retry_time <= now 的记录（最多20条）
        wrapper.eq("event_status", "PENDING_RETRY")
                .le("next_retry_time", LocalDateTime.now())
                .last("limit 20");
        // 遍历记录，尝试重新发送
        List<MqEventRecord> records = mqEventRecordMapper.selectList(wrapper);
        for (MqEventRecord record : records) {
            try {
                // 尝试重新发送消息（resend：失败不重复落库，状态由本条记录承载，避免重试风暴放大表体积）。
                // 超时关单消息必须保档重发：普通 syncSend 会立刻投递，丢掉 20 分钟延时语义；
                // 其余事件仍走普通重发。
                boolean success;
                if (RocketMqReliablePublisher.EVENT_SECKILL_ORDER_TIMEOUT.equals(record.getEventType())) {
                    success = rocketMqReliablePublisher.resendDelay(
                            record.getEventType(),
                            record.getDestination(),
                            record.getPayload(),
                            RocketMqReliablePublisher.ORDER_TIMEOUT_DELAY_LEVEL
                    );
                } else {
                    success = rocketMqReliablePublisher.resend(
                            record.getEventType(),
                            record.getDestination(),
                            record.getPayload()
                    );
                }
                if (success) {
                    // 重试发送成功 → 标记为已发送，不再被后续 Job 扫描到
                    record.setEventStatus("SENT");
                    record.setUpdateTime(LocalDateTime.now());
                    mqEventRecordMapper.updateById(record);
                    continue;
                }
                // 再次失败 → 增加重试计数；未达上限 30 秒后再试，达到上限转 FAILED 终态
                int nextRetryCount = (record.getRetryCount() == null ? 0 : record.getRetryCount()) + 1;
                record.setRetryCount(nextRetryCount);
                record.setLastError("resend failed at attempt " + nextRetryCount);
                record.setUpdateTime(LocalDateTime.now());
                // resolveStatusAfterFailure 根据 nextRetryCount 是否达上限返回 FAILED 或 PENDING_RETRY，
                // 抽出独立方法是为了单测能完整覆盖终态规则（见下方方法注释）
                boolean exhausted = STATUS_FAILED.equals(resolveStatusAfterFailure(nextRetryCount));
                if (exhausted) {
                    // 重试耗尽 → 转 FAILED 终态（不再被 selectList 扫描到），留人工处理入口
                    record.setEventStatus(STATUS_FAILED);
                    log.error("mq event retry exhausted, marked FAILED for manual handling, id={}, retryCount={}",
                            record.getId(), nextRetryCount);
                } else {
                    // 未耗尽 → 重置 nextRetryTime 让下一轮 Job 继续捞（30s 固定间隔，简化版指数退避）
                    record.setNextRetryTime(LocalDateTime.now().plusSeconds(30));
                }
                // 无论转态还是续期，都统一 updateById 落库 —— retryCount/lastError/updateTime 已经在上面更新好了
                mqEventRecordMapper.updateById(record);
                // 只有真正进入 FAILED 终态才记指标，中间过程失败不算，避免监控面板被"正常重试"刷爆
                if (exhausted) {
                    metricsRecorder.incMqEventFailed();
                }
            } catch (Exception e) {
                // 单条异常不影响其他记录，只打 WARN 等待下轮重试
                log.warn("mq event retry failed, id={}", record.getId(), e);
            }
        }
    }

    /**
     * 根据新的重试次数判定事件状态 —— 抽成独立方法便于单测覆盖终态规则。
     *
     * @param retryCount 本次失败后的累计重试次数
     * @return 达到上限返回 FAILED（终态），否则返回 PENDING_RETRY
     */
    String resolveStatusAfterFailure(int retryCount) {
        return retryCount >= maxRetryCount ? STATUS_FAILED : STATUS_PENDING_RETRY;
    }
}