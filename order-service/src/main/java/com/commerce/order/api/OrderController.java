package com.commerce.order.api;

import com.commerce.order.application.OrderFacade;
import com.commerce.order.domain.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderFacade orderFacade;

    public OrderController(OrderFacade orderFacade) {
        this.orderFacade = orderFacade;
    }

    @PostMapping
    public ResponseEntity<?> createOrder(@RequestBody CreateOrderRequest request) {
        try {
            Order order = orderFacade.createOrder(request.userId(), request.totalAmount());
            return ResponseEntity.ok(OrderResponse.from(order));
        } catch (OrderFacade.ConcurrentOrderException e) {
            // 동일 사용자 동시 주문 감지 → 409 Conflict
            return ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage());
        }
    }

    public record CreateOrderRequest(String userId, BigDecimal totalAmount) {}

    public record OrderResponse(Long orderId, String userId, BigDecimal totalAmount, LocalDateTime createdAt) {
        public static OrderResponse from(Order order) {
            return new OrderResponse(order.getId(), order.getUserId(), order.getTotalAmount(), order.getCreatedAt());
        }
    }
}
