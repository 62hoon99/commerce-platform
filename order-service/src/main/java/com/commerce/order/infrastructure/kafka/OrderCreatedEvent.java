package com.commerce.order.infrastructure.kafka;

import java.math.BigDecimal;
import java.time.LocalDateTime;

// Kafka로 직렬화되어 전송되는 이벤트 DTO.
// 도메인 Order 객체를 그대로 전송하지 않는 이유:
// 1. 도메인 객체에 직렬화 어노테이션이 침투하는 것을 막기 위해
// 2. 이벤트 스키마는 소비자와의 계약이므로 도메인 변경과 독립적으로 관리해야 한다
public record OrderCreatedEvent(
        Long orderId,
        String userId,
        BigDecimal totalAmount,
        LocalDateTime createdAt
) {}
