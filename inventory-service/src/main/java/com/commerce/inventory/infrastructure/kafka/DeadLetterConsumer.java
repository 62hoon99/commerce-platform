package com.commerce.inventory.infrastructure.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// DLT(Dead Letter Topic) 소비자.
// 재시도가 모두 소진된 이벤트를 order-events.DLT에서 수신하여 후처리한다.
// 현재는 로그 기록만 수행한다.
// 운영 환경에서는 모니터링 알림, 관리자 대시보드 연동, 수동 재처리 트리거 등을 추가한다.
@Component
public class DeadLetterConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterConsumer.class);

    // DLT 메시지는 원본 bytes이므로 StringDeserializer로 수신한다.
    @KafkaListener(
            topics = "order-events.DLT",
            groupId = "inventory-dlt-group",
            properties = {
                    "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                    "key.deserializer=org.apache.kafka.common.serialization.StringDeserializer"
            }
    )
    public void consume(ConsumerRecord<String, String> record) {
        log.error("[InventoryDLT] 처리 실패 이벤트 수신. topic={}, partition={}, offset={}, key={}\npayload={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
        // TODO: 운영 환경 — 슬랙/PagerDuty 알림, 관리자 수동 재처리 API 연동
    }
}
