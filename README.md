# Commerce Platform

패션 플랫폼 기업들의 기술 블로그를 참고하여 커머스 핵심 도메인을 직접 구현해보는 학습 · 포트폴리오 프로젝트.  
여러 패션 플랫폼의 실제 문제 해결 사례를 직접 구현하며 각 기술의 도입 이유와 트레이드오프를 체감하는 것이 목표다.

---

## 1단계: Kafka — 이벤트 기반 주문 파이프라인

### 왜 이벤트 기반 아키텍처인가?

패션 플랫폼 대규모 세일 이벤트에서 흔히 발생하는 장애 패턴이 있다.  
하나의 서비스(예: 검색)가 응답하지 않으면 주문 API 전체가 멈추는 **강결합(Tight Coupling)** 문제다.

```
강결합 구조 (Before)
──────────────────────────────────────────────────────────────
                  ┌─────────────────┐
POST /api/orders → │  order-service  │──→ inventory-service (동기 HTTP)
                  │                 │──→ notification-service (동기 HTTP)
                  └─────────────────┘

문제: inventory-service 장애 → 주문 자체가 실패
     notification-service 느림 → 주문 응답이 느려짐
     서비스 추가 → order-service 코드 수정 필수
```

```
이벤트 기반 구조 (After)
──────────────────────────────────────────────────────────────
                  ┌─────────────────┐    order-events 토픽
POST /api/orders → │  order-service  │──→ [Kafka] ──→ inventory-service
                  └─────────────────┘         └──→ notification-service

장점: 소비자 서비스 장애가 주문 서비스에 전파되지 않음
     새 서비스 추가 시 order-service 코드 변경 없음
```

### 설계 결정 기록

| 항목 | 선택 | 이유 |
|------|------|------|
| Consumer Group | inventory-group / notification-group 분리 | 동일 이벤트를 두 서비스가 각자 독립적으로 수신 |
| 파티션 키 | orderId | 동일 주문의 이벤트 순서 보장 |
| 파티션 수 | 3 | consumer concurrency=3과 맞춰 최대 병렬 처리량 확보 |
| 도메인 객체 공유 | 안 함 (각 서비스가 독립적으로 DTO 정의) | 서비스 간 강결합 방지 |

---

### Outbox Pattern

**문제**: 주문을 DB에 저장한 직후 Kafka 발행이 실패하면 DB에는 주문이 있지만 재고 감소·알림이 발생하지 않는다. DB 트랜잭션과 Kafka 발행은 서로 다른 시스템이라 하나의 원자적 연산으로 묶을 수 없다.

**왜 Outbox Pattern인가**: 이벤트를 Kafka가 아닌 DB(`outbox_events` 테이블)에 먼저 저장한다. Order 저장과 OutboxEvent 저장이 하나의 DB 트랜잭션에 묶이므로 원자성이 보장된다. 별도 폴러(Relay)가 PENDING 이벤트를 읽어 Kafka에 발행하고 상태를 PUBLISHED로 업데이트한다.

```
주문 생성 트랜잭션 (원자적)
  INSERT INTO orders (...)
  INSERT INTO outbox_events (status='PENDING', payload=...)

OutboxEventPoller (5초마다)
  SELECT * FROM outbox_events WHERE status='PENDING'
  → kafkaTemplate.send() [동기]
  → UPDATE outbox_events SET status='PUBLISHED'
```

**보장 수준**: at-least-once. Kafka 발행 성공 후 DB 커밋 실패 시 동일 이벤트가 재발행될 수 있다. 소비자는 idempotent하게 설계해야 한다. exactly-once를 위해서는 Kafka Transactions API가 필요하다.

---

### Dead Letter Queue (DLT)

**문제**: 소비자(inventory, notification)가 이벤트를 처리하다 예외가 발생하면 기본 동작은 무한 재시도다. 일시적 오류(DB 연결 끊김)는 재시도가 유효하지만, 영구적 오류(잘못된 데이터 형식)는 재시도해도 영원히 실패하여 파티션 처리가 중단된다.

**왜 DLT인가**: 재시도를 정해진 횟수(3회, 지수 백오프 1초→2초→4초)로 제한하고 모두 실패한 이벤트를 `order-events.DLT` 토픽으로 라우팅한다. DLT 소비자가 실패 이벤트를 별도로 수신하여 로그·알림을 남기고, 이후 수동 재처리나 보상 트랜잭션을 적용할 수 있다. 파티션 처리가 차단되지 않아 정상 이벤트 처리가 계속된다.

```
이벤트 처리 실패
  → 1차 재시도 (1초 후)
  → 2차 재시도 (2초 후)
  → 3차 재시도 (4초 후)
  → order-events.DLT 라우팅
  → DeadLetterConsumer 수신 (로그·알림)
```

---

## 프로젝트 구조

```
commerce-platform/
├── docker-compose.yml
├── build.gradle.kts
├── settings.gradle.kts
├── scripts/
│   └── mysql-init.sql
├── order-service/            (port: 8080)
│   └── domain/
│       ├── Order.java
│       ├── OutboxEvent.java           # Outbox Pattern 엔티티
│       └── OutboxEventRepository.java
│   └── infrastructure/
│       ├── kafka/OrderEventPublisher.java
│       └── outbox/OutboxEventPoller.java  # PENDING 이벤트 Kafka 발행 스케줄러
├── inventory-service/        (port: 8081)
│   └── infrastructure/kafka/
│       ├── OrderEventConsumer.java
│       ├── KafkaConsumerConfig.java   # DLQ ErrorHandler
│       └── DeadLetterConsumer.java    # DLT 소비자
└── notification-service/     (port: 8082)
    └── infrastructure/kafka/
        ├── OrderEventConsumer.java
        ├── KafkaConsumerConfig.java
        └── DeadLetterConsumer.java
```

---

## 로컬 실행

### 1. 인프라 실행

```bash
docker-compose up -d
```

| 서비스 | 포트 |
|--------|------|
| Kafka | 9092 |
| Zookeeper | 2181 |
| MySQL | 3306 |
| Redis | 6379 |
| Kafka UI | 8989 |

### 2. 서비스 실행

```bash
# 별도 터미널 3개에서 각각 실행
./gradlew :order-service:bootRun
./gradlew :inventory-service:bootRun
./gradlew :notification-service:bootRun
```

### 3. 주문 생성

```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-123", "totalAmount": 59000}'
```

### 4. 확인 포인트

| 항목 | 확인 방법 |
|------|----------|
| Kafka 메시지 | http://localhost:8989 → Topics → order-events |
| Outbox 상태 | MySQL `order_db.outbox_events` 테이블 status 컬럼 |
| 재고 처리 | inventory-service 로그: `[InventoryConsumer] 재고 감소 처리` |
| 알림 처리 | notification-service 로그: `[NotificationConsumer] 알림 발송` |
| DLT 메시지 | Kafka UI → Topics → order-events.DLT |

---

## 로드맵

- [x] **1단계**: Kafka 이벤트 기반 파이프라인 + Outbox Pattern + Dead Letter Queue
- [ ] **2단계**: Redis — 실시간 인기 상품 랭킹 (Sorted Set), 분산락 (Redisson), Circuit Breaker
- [ ] **3단계**: Elasticsearch — 상품 검색 (MySQL LIKE → BM25)

---

## 참고 자료

- 패션 플랫폼 기업 기술 블로그
- [Apache Kafka 공식 문서](https://kafka.apache.org/documentation/)
- [Redisson 공식 문서](https://redisson.org/documentation.html)
