package com.commerce.inventory.infrastructure.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

// @KafkaListener의 concurrency 설정 이유:
// order-events 토픽은 파티션 3개로 구성되어 있다.
// concurrency=3으로 설정하면 3개의 컨슈머 스레드가 각 파티션을 담당하여
// 최대 병렬 처리량을 확보한다. 파티션 수보다 많은 concurrency는 의미 없다.
@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    @KafkaListener(
            topics = "order-events",
            groupId = "inventory-group",
            concurrency = "3"
    )
    public void consume(
            OrderCreatedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("[InventoryConsumer] 이벤트 수신. orderId={}, userId={}, amount={}, partition={}, offset={}",
                event.orderId(), event.userId(), event.totalAmount(), partition, offset);

        // 실제 재고 감소 로직은 여기에 구현한다.
        // 현재는 학습 목적으로 로그 출력만 한다.
        decreaseStock(event);
    }

    private void decreaseStock(OrderCreatedEvent event) {
        log.info("[InventoryConsumer] 재고 감소 처리. orderId={}", event.orderId());
        // TODO: 재고 DB 업데이트 로직 구현
    }
}
