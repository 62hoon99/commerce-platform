package com.commerce.notification.infrastructure.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DeadLetterConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterConsumer.class);

    @KafkaListener(
            topics = "order-events.DLT",
            groupId = "notification-dlt-group",
            properties = {
                    "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                    "key.deserializer=org.apache.kafka.common.serialization.StringDeserializer"
            }
    )
    public void consume(ConsumerRecord<String, String> record) {
        log.error("[NotificationDLT] 처리 실패 이벤트 수신. topic={}, partition={}, offset={}, key={}\npayload={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
        // TODO: 운영 환경 — 슬랙/PagerDuty 알림, 관리자 수동 재처리 API 연동
    }
}
