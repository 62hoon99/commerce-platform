package com.commerce.notification.infrastructure.kafka;

import java.math.BigDecimal;
import java.time.LocalDateTime;

// inventory-service의 OrderCreatedEvent와 동일하게, 서비스 간 직접 공유 없이
// 각자 이벤트 스키마를 독립적으로 정의한다.
public record OrderCreatedEvent(
        Long orderId,
        String userId,
        BigDecimal totalAmount,
        LocalDateTime createdAt
) {}
