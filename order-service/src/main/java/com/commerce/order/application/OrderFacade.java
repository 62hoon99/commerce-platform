package com.commerce.order.application;

import com.commerce.order.domain.Order;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

// [왜 OrderFacade를 별도로 두는가?]
// 분산락은 Redis에 의존하는 인프라 관심사다.
// OrderService(도메인/애플리케이션 로직)가 Redis를 직접 알게 되면
// CLAUDE.md 원칙("도메인 레이어는 외부 기술에 의존하지 않도록 분리")을 위반한다.
// OrderFacade가 락을 잡고 OrderService에 위임함으로써 관심사를 분리한다.
//
// [왜 분산락이 필요한가 — 문제 상황]
//
// 문제 1: 동일 사용자 동시 주문
//   패션 플랫폼 세일 이벤트에서 사용자가 "구매" 버튼을 빠르게 두 번 클릭하면
//   두 요청이 거의 동시에 서버에 도달한다. 두 요청 모두 검증을 통과하면
//   동일한 주문이 두 건 생성되어 사용자가 이중으로 결제된다.
//
// 문제 2: 다중 인스턴스의 JVM 락 한계
//   Java의 synchronized / ReentrantLock은 같은 JVM 내에서만 유효하다.
//   트래픽이 몰려 order-service를 3개 인스턴스로 스케일 아웃하면
//   각 인스턴스의 JVM 락은 서로를 인식하지 못한다.
//   두 인스턴스에서 동시에 같은 userId로 주문이 들어오면 락이 무력화된다.
//
// 해결: Redis 분산락
//   모든 인스턴스가 공유하는 Redis에서 락을 관리하므로
//   인스턴스 수에 관계없이 동시성이 보장된다.
@Component
public class OrderFacade {

    private static final Logger log = LoggerFactory.getLogger(OrderFacade.class);

    // [락 키 설계: 사용자 단위]
    // lock:order:user:{userId}: 같은 사용자의 동시 요청만 막고,
    // 다른 사용자의 주문은 독립적으로 병렬 처리된다.
    // lock:order: 네임스페이스로 다른 목적의 락과 충돌을 방지한다.
    private static final String LOCK_KEY_PREFIX = "lock:order:user:";

    // [leaseTime: 5초]
    // 락을 획득한 후 5초 내에 처리가 완료되지 않으면 Redis가 자동으로 락을 해제한다.
    // 예외가 발생해 finally 블록도 실행되지 않는 극단적 상황의 데드락 방지가 목적.
    // 정상적인 주문 생성(DB 저장 + OutboxEvent 저장)은 수십~수백 ms 내에 완료된다.
    private static final long LEASE_TIME_SECONDS = 5;

    private final OrderService orderService;
    private final RedissonClient redissonClient;

    public OrderFacade(OrderService orderService, RedissonClient redissonClient) {
        this.orderService = orderService;
        this.redissonClient = redissonClient;
    }

    public Order createOrder(String userId, BigDecimal totalAmount) {
        String lockKey = LOCK_KEY_PREFIX + userId;
        RLock lock = redissonClient.getLock(lockKey);

        boolean acquired;
        try {
            // tryLock(waitTime=0, leaseTime=5s, unit)
            // waitTime=0: 락을 즉시 획득하지 못하면 대기 없이 바로 false 반환.
            // 대기를 허용하면(waitTime > 0) 동시 요청들이 큐처럼 쌓여
            // 응답이 지연되고 결국 동일한 주문이 순차 생성될 수 있다.
            // 중복 주문은 "나중에 재시도"가 아니라 "즉시 실패"로 처리하는 것이 맞다.
            acquired = lock.tryLock(0, LEASE_TIME_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("락 획득 중 인터럽트 발생", e);
        }

        if (!acquired) {
            log.warn("[OrderFacade] 동일 사용자 동시 주문 감지, 락 획득 실패. userId={}", userId);
            throw new ConcurrentOrderException("이미 주문 처리 중입니다. 잠시 후 다시 시도해주세요.");
        }

        try {
            return orderService.createOrder(userId, totalAmount);
        } finally {
            // isHeldByCurrentThread: 현재 스레드가 락을 보유 중인지 확인 후 해제.
            // leaseTime 초과로 자동 해제된 경우 unlock()을 호출하면 예외가 발생하므로
            // 반드시 보유 여부를 확인한다.
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    public static class ConcurrentOrderException extends RuntimeException {
        public ConcurrentOrderException(String message) {
            super(message);
        }
    }
}
