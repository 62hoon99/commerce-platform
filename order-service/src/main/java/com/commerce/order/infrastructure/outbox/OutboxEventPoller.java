package com.commerce.order.infrastructure.outbox;

import com.commerce.order.domain.OutboxEvent;
import com.commerce.order.domain.OutboxEventRepository;
import com.commerce.order.domain.OutboxEventStatus;
import com.commerce.order.infrastructure.kafka.OrderEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

// Outbox Pattern의 폴러(Relay) 컴포넌트.
// PENDING 상태 이벤트를 주기적으로 읽어 Kafka에 발행하고 상태를 업데이트한다.
// @Transactional: Kafka 발행 성공 후 DB 상태 업데이트를 원자적으로 처리한다.
//   Kafka 발행은 성공했지만 DB 커밋이 실패하면 이벤트가 재발행(at-least-once)된다.
//   완전한 exactly-once를 위해서는 Kafka Transactions API가 필요하다.
@Component
public class OutboxEventPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxEventPoller.class);
    private static final int MAX_RETRY = 3;

    private final OutboxEventRepository outboxEventRepository;
    private final OrderEventPublisher orderEventPublisher;

    public OutboxEventPoller(OutboxEventRepository outboxEventRepository,
                             OrderEventPublisher orderEventPublisher) {
        this.outboxEventRepository = outboxEventRepository;
        this.orderEventPublisher = orderEventPublisher;
    }

    // fixedDelay: 이전 실행 완료 후 5초 대기, 중복 실행 방지
    // 다중 인스턴스 환경에서는 Redisson 분산락으로 하나의 인스턴스만 실행되도록 제어해야 한다. (2단계 예정)
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void poll() {
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
    }
}
