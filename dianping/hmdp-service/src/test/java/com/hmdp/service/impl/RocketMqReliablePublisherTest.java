package com.hmdp.service.impl;

import com.hmdp.entity.MqEventRecord;
import com.hmdp.mapper.MqEventRecordMapper;
import com.hmdp.metrics.MetricsRecorder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.messaging.Message;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class RocketMqReliablePublisherTest {

    @Test
    void publish_shouldPersistFailureRecordWhenSendThrows() {
        RocketMQTemplate rocketMQTemplate = mock(RocketMQTemplate.class);
        MqEventRecordMapper mapper = mock(MqEventRecordMapper.class);
        RocketMqReliablePublisher publisher = new RocketMqReliablePublisher(rocketMQTemplate, mapper, new MetricsRecorder(new SimpleMeterRegistry()));
        doThrow(new RuntimeException("mq down")).when(rocketMQTemplate)
                .syncSend(eq("topic:tag"), any(Message.class));

        boolean success = publisher.publish("SECKILL_ORDER_CREATE", "topic:tag", "{\"id\":1}");

        assertFalse(success);
        verify(mapper).insert(any(MqEventRecord.class));
    }

    @Test
    void publish_shouldReturnTrueWhenSendSucceeds() {
        RocketMQTemplate rocketMQTemplate = mock(RocketMQTemplate.class);
        MqEventRecordMapper mapper = mock(MqEventRecordMapper.class);
        RocketMqReliablePublisher publisher = new RocketMqReliablePublisher(rocketMQTemplate, mapper, new MetricsRecorder(new SimpleMeterRegistry()));

        boolean success = publisher.publish("SECKILL_ORDER_CREATE", "topic:tag", "{\"id\":1}");

        assertTrue(success);
    }

    @Test
    void publishDelay_shouldSubmitAsyncAndPersistOnCallbackFailure() {
        RocketMQTemplate rocketMQTemplate = mock(RocketMQTemplate.class);
        MqEventRecordMapper mapper = mock(MqEventRecordMapper.class);
        RocketMqReliablePublisher publisher = new RocketMqReliablePublisher(rocketMQTemplate, mapper, new MetricsRecorder(new SimpleMeterRegistry()));

        // 提交阶段返回 true（异步化语义：不再等 Broker ACK）
        boolean submitted = publisher.publishDelay("SECKILL_ORDER_TIMEOUT", "topic:tag", "{\"id\":1}", 15);
        assertTrue(submitted);

        // 异步发送失败 → 回调里必须落 tb_mq_event_record 兜底（重试 Job 依赖它）
        ArgumentCaptor<SendCallback> callbackCaptor = ArgumentCaptor.forClass(SendCallback.class);
        verify(rocketMQTemplate).asyncSend(eq("topic:tag"), any(Message.class), callbackCaptor.capture());
        callbackCaptor.getValue().onException(new RuntimeException("mq down"));
        verify(mapper).insert(any(MqEventRecord.class));
    }

    @Test
    void publishDelay_shouldPersistAndReturnFalseWhenSubmitThrows() {
        RocketMQTemplate rocketMQTemplate = mock(RocketMQTemplate.class);
        MqEventRecordMapper mapper = mock(MqEventRecordMapper.class);
        RocketMqReliablePublisher publisher = new RocketMqReliablePublisher(rocketMQTemplate, mapper, new MetricsRecorder(new SimpleMeterRegistry()));
        doThrow(new RuntimeException("mq down")).when(rocketMQTemplate)
                .asyncSend(eq("topic:tag"), any(Message.class), any(SendCallback.class));

        boolean success = publisher.publishDelay("SECKILL_ORDER_TIMEOUT", "topic:tag", "{\"id\":1}", 15);

        assertFalse(success);
        verify(mapper).insert(any(MqEventRecord.class));
    }

    @Test
    void resendDelay_shouldStaySyncForRetryJob() {
        RocketMQTemplate rocketMQTemplate = mock(RocketMQTemplate.class);
        MqEventRecordMapper mapper = mock(MqEventRecordMapper.class);
        RocketMqReliablePublisher publisher = new RocketMqReliablePublisher(rocketMQTemplate, mapper, new MetricsRecorder(new SimpleMeterRegistry()));
        doThrow(new RuntimeException("mq down")).when(rocketMQTemplate)
                .syncSend(eq("topic:tag"), any(Message.class));

        // 重试路径刻意保持同步：Job 依赖同步返回值维护原记录状态，失败不落库（防重试风暴）
        boolean success = publisher.resendDelay("SECKILL_ORDER_TIMEOUT", "topic:tag", "{\"id\":1}", 15);

        assertFalse(success);
        verify(mapper, never()).insert(any(MqEventRecord.class));
    }
}
