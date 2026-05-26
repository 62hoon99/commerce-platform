package com.commerce.order.infrastructure.outbox;

import com.commerce.order.domain.OutboxEvent;
import com.commerce.order.domain.OutboxEventRepository;
import com.commerce.order.domain.OutboxEventStatus;
import com.commerce.order.infrastructure.kafka.OrderEventPublisher;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.TimeUnit;

// Outbox Pattern의 폴러(Relay) 컴포넌트.
// PENDING 상태 이벤트를 주기적으로 읽어 Kafka에 발행하고 상태를 업데이트한다.
//
// [왜 OutboxEventPoller에도 분산락이 필요한가?]
// order-service를 여러 인스턴스로 수평 확장하면 각 인스턴스가 독립적으로 스케줄러를 실행한다.
//
//   [인스턴스 A] poll() → PENDING 이벤트 50건 조회 → Kafka 발행
//   [인스턴스 B] poll() → 같은 50건 조회 → Kafka 발행 (중복!)
//
// 결과: Kafka에 동일 이벤트가 2~3배 발행되어 inventory-service가
// 재고를 N배 감소시키는 치명적 데이터 불일치가 발생한다.
//
// 분산락으로 "전역적으로" 단 하나의 인스턴스만 폴링하도록 보장한다.
@Component
public class OutboxEventPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxEventPoller.class);
    private static final int MAX_RETRY = 3;

    // [락 키 설계: 전역 단일 키]
    // OutboxEventPoller는 어떤 인스턴스 하나만 실행되면 충분하므로
    // 사용자별이 아닌 서비스 전역 단일 키를 사용한다.
    private static final String POLLER_LOCK_KEY = "lock:outbox:poller";

    // [leaseTime: 30초]
    // poll() 한 번의 최대 실행 시간보다 여유 있게 설정한다.
    // 최대 50건 × 이벤트당 최대 500ms = 25초 → 30초는 충분한 여유.
    // leaseTime 내에 완료되면 finally에서 명시적으로 해제한다.
    // 인스턴스가 갑자기 죽어 finally가 실행되지 않아도 30초 후 자동 해제된다.
    private static final long POLLER_LEASE_SECONDS = 30;

    private final OutboxEventRepository outboxEventRepository;
    private final OrderEventPublisher orderEventPublisher;
    private final RedissonClient redissonClient;

    public OutboxEventPoller(OutboxEventRepository outboxEventRepository,
                             OrderEventPublisher orderEventPublisher,
                             RedissonClient redissonClient) {
        this.outboxEventRepository = outboxEventRepository;
        this.orderEventPublisher = orderEventPublisher;
        this.redissonClient = redissonClient;
    }

    // fixedDelay: 이전 실행 완료 후 5초 대기.
    // @Transactional: Kafka 발행 성공 후 DB 상태 업데이트를 원자적으로 처리한다.
    //   at-least-once 보장: Kafka 발행 성공 후 DB 커밋 실패 시 이벤트가 재발행될 수 있다.
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void poll() {
        RLock lock = redissonClient.getLock(POLLER_LOCK_KEY);

        boolean acquired;
        try {
            // waitTime=0: 락 획득 실패 시 즉시 이번 사이클을 건너뜀.
            // 다른 인스턴스가 이미 처리 중이라는 의미이므로 기다릴 이유가 없다.
            acquired = lock.tryLock(0, POLLER_LEASE_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        if (!acquired) {
            log.debug("[OutboxPoller] 다른 인스턴스가 폴링 중, 이번 사이클 건너뜀");
            return;
        }

        try {
            List<OutboxEvent> pendingEvents =
                    outboxEventRepository.findTop50ByStatusOrderByCreatedAtAsc(OutboxEventStatus.PENDING);

            if (pendingEvents.isEmpty()) {
                return;
            }

            log.debug("[OutboxPoller] PENDING 이벤트 {}건 처리 시작", pendingEvents.size());

            for (OutboxEvent outboxEvent : pendingEvents) {
                try {
                    orderEventPublisher.publishSync(outboxEvent);
                    outboxEvent.markPublished();
                } catch (Exception e) {
                    outboxEvent.incrementRetryCount();
                    if (outboxEvent.getRetryCount() >= MAX_RETRY) {
                        outboxEvent.markFailed();
                        log.error("[OutboxPoller] 최대 재시도 초과, FAILED 처리. outboxId={}, aggregateId={}",
                                outboxEvent.getId(), outboxEvent.getAggregateId());
                    } else {
                        log.warn("[OutboxPoller] 발행 실패, 재시도 예정. outboxId={}, retryCount={}, error={}",
                                outboxEvent.getId(), outboxEvent.getRetryCount(), e.getMessage());
                    }
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}
