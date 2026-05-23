package com.commerce.order.api;

import com.commerce.order.application.OrderService;
import com.commerce.order.domain.Order;
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

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@RequestBody CreateOrderRequest request) {
        Order order = orderService.createOrder(request.userId(), request.totalAmount());
        return ResponseEntity.ok(OrderResponse.from(order));
    }

    public record CreateOrderRequest(String userId, BigDecimal totalAmount) {}

    public record OrderResponse(Long orderId, String userId, BigDecimal totalAmount, LocalDateTime createdAt) {
        public static OrderResponse from(Order order) {
            return new OrderResponse(order.getId(), order.getUserId(), order.getTotalAmount(), order.getCreatedAt());
        }
    }
}
