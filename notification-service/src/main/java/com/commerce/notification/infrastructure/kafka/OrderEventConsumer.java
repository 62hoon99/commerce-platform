package com.commerce.notification.infrastructure.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    @KafkaListener(
            topics = "order-events",
            groupId = "notification-group",
            concurrency = "3"
    )
    public void consume(
            OrderCreatedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("[NotificationConsumer] 이벤트 수신. orderId={}, userId={}, amount={}, partition={}, offset={}",
                event.orderId(), event.userId(), event.totalAmount(), partition, offset);

        sendNotification(event);
    }

    private void sendNotification(OrderCreatedEvent event) {
        log.info("[NotificationConsumer] 알림 발송. userId={}, orderId={}", event.userId(), event.orderId());
        // TODO: 실제 알림 발송 로직 구현 (이메일, 푸시 알림 등)
    }
}
