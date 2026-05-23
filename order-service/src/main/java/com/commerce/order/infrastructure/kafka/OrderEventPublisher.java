package com.commerce.order.infrastructure.kafka;

import com.commerce.order.domain.OutboxEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

// Outbox Pattern 적용 후 이 클래스는 OutboxEventPoller에서만 호출된다.
// HTTP 요청 스레드가 아닌 스케줄러 스레드에서 실행되므로
// 동기 발행(kafkaTemplate.send().get())을 사용해도 HTTP 응답에 영향이 없다.
// 동기 발행의 장점: 성공/실패를 즉시 알 수 있어 OutboxEvent 상태 업데이트가 단순해진다.
@Component
public class OrderEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);
    private static final long SEND_TIMEOUT_SECONDS = 5L;

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topic.order-events}")
    private String orderEventsTopic;

    public OrderEventPublisher(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate,
                               ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publishSync(OutboxEvent outboxEvent) throws Exception {
        OrderCreatedEvent event = objectMapper.readValue(outboxEvent.getPayload(), OrderCreatedEvent.class);

        // 파티션 키로 aggregateId(orderId)를 사용한다.
        // 동일한 주문의 이벤트가 항상 같은 파티션으로 라우팅되어 순서가 보장된다.
        String partitionKey = String.valueOf(outboxEvent.getAggregateId());

        kafkaTemplate.send(orderEventsTopic, partitionKey, event)
                .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        log.info("[OrderEventPublisher] 발행 성공. outboxId={}, aggregateId={}",
                outboxEvent.getId(), outboxEvent.getAggregateId());
    }
}
