package com.commerce.order.domain;

public enum OutboxEventStatus {
    PENDING,
    PUBLISHED,
    FAILED
}
