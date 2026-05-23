package com.commerce.inventory.infrastructure.kafka;

import java.math.BigDecimal;
import java.time.LocalDateTime;

// order-service가 발행하는 OrderCreatedEvent와 동일한 구조의 소비자 측 DTO.
// 서비스 간 도메인 객체를 직접 공유하지 않는 이유:
// order-service 내부 도메인 변경이 inventory-service에 강결합을 만들기 때문이다.
// 이벤트 스키마(계약)만 맞추면 각 서비스는 독립적으로 진화할 수 있다.
public record OrderCreatedEvent(
        Long orderId,
        String userId,
        BigDecimal totalAmount,
        LocalDateTime createdAt
) {}
