package com.commerce.order.application;

import com.commerce.order.domain.Order;
import com.commerce.order.domain.OrderRepository;
import com.commerce.order.domain.OutboxEvent;
import com.commerce.order.domain.OutboxEventRepository;
import com.commerce.order.infrastructure.kafka.OrderCreatedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository,
                        OutboxEventRepository outboxEventRepository,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Order createOrder(String userId, BigDecimal totalAmount) {
        Order order = Order.create(userId, totalAmount);
        Order savedOrder = orderRepository.save(order);

        // Outbox Pattern: Order 저장과 OutboxEvent 저장을 하나의 트랜잭션으로 묶는다.
        // DB 커밋이 성공하면 outbox_events에 반드시 레코드가 존재하고,
        // 실패하면 rollback되어 Kafka 발행 없이 데이터 불일치가 방지된다.
        saveOutboxEvent(savedOrder);

        return savedOrder;
    }

    private void saveOutboxEvent(Order order) {
        try {
            OrderCreatedEvent event = new OrderCreatedEvent(
                    order.getId(), order.getUserId(),
                    order.getTotalAmount(), order.getCreatedAt());
            String payload = objectMapper.writeValueAsString(event);
            outboxEventRepository.save(OutboxEvent.of(order.getId(), payload));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("OutboxEvent payload 직렬화 실패", e);
        }
    }
}
