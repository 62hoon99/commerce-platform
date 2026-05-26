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

## 2단계: Redis 실시간 랭킹 + Circuit Breaker

### 왜 MySQL이 아니라 Redis인가?

랭킹을 MySQL로 집계하면 상품을 조회할 때마다 `UPDATE products SET view_count = view_count + 1`이 발생한다.  
대규모 세일처럼 초당 수천 건의 조회가 발생하면 이 UPDATE가 row-level lock을 유발해 쿼리 경합으로 이어진다.

Redis Sorted Set은 이 문제를 두 가지 방식으로 해결한다.

| 비교 항목 | MySQL | Redis Sorted Set |
|-----------|-------|-----------------|
| 조회수 증가 | `UPDATE` + row lock | `ZINCRBY` 단일 원자 명령 |
| 랭킹 정렬 | `ORDER BY view_count DESC` (풀스캔 또는 인덱스) | Skip List 기반 O(log N) |
| 저장소 | 디스크 I/O 포함 | 인메모리 |
| 동시성 처리 | 낙관적/비관적 락 | 단일 스레드 이벤트 루프 (락 불필요) |

랭킹은 "영구적으로 정확한 기록"보다 "빠른 실시간 반영"이 중요한 케이스다.  
Redis는 이 트레이드오프에서 명확한 우위에 있다.

---

### Sorted Set(ZSet)을 선택한 이유

Redis의 여러 자료구조 중 Sorted Set을 선택한 이유:

- **String**: 단순 카운터는 가능하지만 여러 상품을 정렬해서 가져올 수 없다.
- **List**: 삽입 순서 유지는 되지만 score 기반 정렬이 없다.
- **Sorted Set**: `(member, score)` 쌍으로 저장하고 score 기준 정렬을 자동으로 유지한다.
  - `ZINCRBY key 1 productId` → 조회수 1 증가 + 순위 자동 갱신
  - `ZREVRANGE key 0 9 WITHSCORES` → score 내림차순 TOP 10 조회

---

### ZINCRBY 원자성과 동시성

Redis는 단일 스레드 이벤트 루프로 모든 명령을 순차 처리한다.  
`ZINCRBY`는 "읽기 → 증가 → 쓰기"가 하나의 원자적 명령으로 실행된다.

```
[서버 A] ZINCRBY ranking:daily:20260525 1 "productId:1"
[서버 B] ZINCRBY ranking:daily:20260525 1 "productId:1"
                          ↓
          Redis 이벤트 루프: A 실행 → B 실행 (순차, 끼어들 수 없음)
          결과: score = 2 (정확)
```

MySQL의 `UPDATE ... SET view_count = view_count + 1`은 두 트랜잭션이 동시에 같은 값을 읽으면  
둘 다 같은 값으로 +1하는 Lost Update가 발생할 수 있다.  
Redis는 구조적으로 이 문제가 없다.

---

### 랭킹 키 날짜 전략과 TTL

```
ranking:daily          (고정 키)
ranking:daily:20260525 (날짜 포함 키) ← 채택
```

**고정 키의 문제**: TTL이 키 생성 시점부터 카운트되므로 "자정 리셋"이 보장되지 않는다.  
오전 6시에 키가 만들어지면 24시간 TTL 기준으로 다음날 오전 6시에 만료된다. 날짜가 어긋난다.

**날짜 포함 키**: 날짜가 바뀌면 자연스럽게 새 키를 사용하므로 일별 랭킹이 독립적으로 쌓인다.  
TTL 48시간은 자정 전후 경계에서 키가 사라지는 것을 막고, 당일 오전에 전날 랭킹도 참조 가능하게 한다.  
무한정 누적을 막는 Redis 메모리 자동 정리 역할도 한다.

TTL 설정은 키가 처음 생성될 때(`getExpire == -1`)만 한다. 매 조회마다 호출하면 TTL이 계속 리셋된다.

---

### Circuit Breaker 도입 이유

퀸잇의 럭퀸세일처럼 대규모 트래픽이 몰릴 때 Redis 응답이 지연되거나 다운되면 다음 흐름으로 전체 장애가 발생한다.

```
Redis 응답 지연
  → 랭킹 API 스레드가 타임아웃까지 블로킹
  → 스레드 풀 소진
  → 다른 정상 API도 처리 불가
  → 서비스 전체 다운  ← Cascading Failure
```

Circuit Breaker는 이 전파를 차단한다.

```
Redis 응답 지연
  → Circuit Breaker: OPEN 상태 진입
  → 이후 요청은 Redis에 가지 않고 즉시 fallback(빈 리스트) 반환
  → 스레드 블로킹 없음 → 나머지 API 정상 동작 유지
```

랭킹이 잠깐 안 보이는 것은 감수할 수 있는 손실이지만, 전체 서비스 다운은 허용할 수 없다.  
Circuit Breaker는 이 트레이드오프를 명시적으로 선택하는 패턴이다.

---

### Circuit Breaker 세 가지 상태

```
                 실패율 >= 50%
   CLOSED ──────────────────────→ OPEN
     ↑                              │
     │ 테스트 성공                  │ 10초 경과
     │                              ↓
     └─────────────────────── HALF_OPEN
                                    │
                              테스트 실패
                                    │
                                   OPEN (재진입)
```

| 상태 | 동작 | 전환 조건 |
|------|------|-----------|
| **CLOSED** | 모든 요청을 Redis로 통과. 실패율 측정 중 | 실패율 ≥ 50% (10번 중 5번 이상 실패) |
| **OPEN** | 모든 요청을 즉시 차단. Redis에 연결 안 함. fallback 반환 | 10초 경과 후 HALF_OPEN |
| **HALF_OPEN** | 소수(3번)의 테스트 요청만 허용해 복구 여부 확인 | 성공 → CLOSED / 실패 → OPEN |

---

### 로컬 테스트 방법

#### 정상 흐름

```bash
# product-service 실행 (포트 8083)
./gradlew :product-service:bootRun

# 상품별 다른 횟수로 조회 (조회 = Redis score 증가)
for i in {1..5}; do curl -s http://localhost:8083/api/products/1 > /dev/null; done
for i in {1..3}; do curl -s http://localhost:8083/api/products/2 > /dev/null; done
for i in {1..7}; do curl -s http://localhost:8083/api/products/3 > /dev/null; done

# TOP 10 랭킹 조회
curl http://localhost:8083/api/products/ranking

# redis-cli로 직접 확인
docker exec commerce-redis redis-cli ZREVRANGE ranking:daily:$(date +%Y%m%d) 0 -1 WITHSCORES
docker exec commerce-redis redis-cli TTL ranking:daily:$(date +%Y%m%d)
```

#### Circuit Breaker 확인

```bash
# 1. Redis 중지
docker stop commerce-redis

# 2. 랭킹 API 반복 호출 → 실패 후 fallback(빈 배열) 반환, 회로 OPEN 전환 확인
for i in {1..10}; do curl -s http://localhost:8083/api/products/ranking; echo ""; done

# 3. Redis 재시작 (10초 경과 후 HALF_OPEN → CLOSED 복구)
docker start commerce-redis
sleep 12
curl http://localhost:8083/api/products/ranking  # 정상 랭킹 반환
```

---

## 2단계 (계속): Redisson 분산락 — 주문 동시성 제어

### 어떤 문제를 해결하는가?

Redisson 분산락이 필요한 문제는 두 가지다.

---

#### 문제 1: OutboxEventPoller 중복 실행 (다중 인스턴스)

패션 플랫폼 대규모 세일에서 트래픽이 몰리면 order-service를 여러 인스턴스로 수평 확장한다.  
각 인스턴스는 자신의 `OutboxEventPoller`를 5초마다 독립적으로 실행한다.

```
[인스턴스 A] poll() → PENDING 이벤트 50건 조회 → Kafka 50건 발행
[인스턴스 B] poll() → 같은 50건 조회 (A가 아직 커밋 전) → Kafka 50건 발행 (중복!)
[인스턴스 C] poll() → 같은 50건 조회 → Kafka 50건 발행 (중복!)

결과: Kafka에 150건이 발행됨 → inventory-service가 재고를 3배 감소시킴
```

이것은 데이터 불일치로 이어지는 심각한 장애다.  
Outbox Pattern이 at-least-once를 보장하더라도, **발행 중복**은 별도로 제어해야 한다.

---

#### 문제 2: 동일 사용자 동시 주문 중복 생성

사용자가 "구매" 버튼을 빠르게 두 번 클릭하면 두 요청이 거의 동시에 서버에 도달한다.  
두 요청 모두 검증을 통과하면 동일한 주문이 두 건 생성되어 사용자가 이중 결제된다.

```
[요청 A] createOrder(userId=user-1) → 검증 통과 → 주문 저장 (orderId=1)
[요청 B] createOrder(userId=user-1) → 검증 통과 → 주문 저장 (orderId=2) ← 중복!

결과: 사용자는 동일한 주문을 두 번 결제
```

---

### 왜 JVM 락(synchronized)으로는 부족한가?

```java
// 이 코드는 단일 인스턴스에서만 동작한다
synchronized (userId.intern()) {
    orderService.createOrder(userId, totalAmount);
}
```

`synchronized`와 `ReentrantLock`은 **같은 JVM 내에서만** 유효하다.  
인스턴스가 여러 개라면 각 인스턴스의 JVM 락은 서로를 전혀 인식하지 못한다.

```
[인스턴스 A JVM] synchronized(user-1) { 처리 중 }
[인스턴스 B JVM] synchronized(user-1) { 동시에 처리 중 } ← A의 락을 모름
```

스케일 아웃하는 순간 JVM 락은 효력을 잃는다.

---

### 왜 DB 비관적 락(SELECT FOR UPDATE)으로는 부족한가?

```sql
SELECT * FROM orders WHERE user_id = ? FOR UPDATE
```

DB 락도 동시성 제어는 가능하다. 하지만 다음 문제가 있다.

| 항목 | DB 비관적 락 | Redis 분산락 |
|------|------------|-------------|
| 속도 | 디스크 I/O + 쿼리 파싱 | 인메모리 서브밀리초 |
| 병목 | DB 락 관리가 병목 | Redis가 독립적으로 처리 |
| 커넥션 | 락 대기 중 DB 커넥션 점유 | DB 커넥션 미점유 |
| 스코프 | 트랜잭션 범위 내 | 명시적 제어 가능 |

대규모 세일처럼 동시 요청이 폭발할 때 DB 커넥션 풀이 락 대기 요청으로 소진되면  
정상적인 DB 쿼리까지 처리하지 못해 전체 서비스가 멈춘다.  
DB는 이미 주문 저장, OutboxEvent 저장으로 바쁜데 락 관리 부담까지 주는 것은 좋지 않다.

---

### 왜 Redisson(Redis 분산락)인가?

Redis는 이미 랭킹 기능으로 인프라에 존재한다. 추가 비용 없이 분산락을 구현할 수 있다.

```
모든 인스턴스가 Redis를 공유 → 인스턴스가 몇 개든 락이 전역적으로 동작
```

Redisson이 제공하는 핵심 특성:

- **인메모리**: 락 획득/해제가 서브밀리초. DB 락 대비 수십~수백 배 빠름
- **leaseTime (자동 만료)**: 락을 획득한 프로세스가 비정상 종료되어도 leaseTime 후 자동 해제 → 데드락 없음
- **tryLock(waitTime=0)**: 락 획득 실패 시 즉시 반환 → 요청이 큐처럼 쌓이지 않음
- **isHeldByCurrentThread()**: 현재 스레드가 락을 보유 중인지 확인 → 안전한 해제 가능

---

### 구현 내용

#### OrderFacade — 사용자별 락

```
락 키: lock:order:user:{userId}
waitTime: 0s  (즉시 실패, 대기 없음)
leaseTime: 5s (주문 생성은 수백 ms, 5초면 충분)
```

```
[요청 A] lock:order:user:user-1 획득 → 주문 생성 → 락 해제
[요청 B] lock:order:user:user-1 획득 실패 → 409 Conflict 즉시 반환
[요청 C] lock:order:user:user-2 획득 → 주문 생성 (user-1과 무관하게 병렬 처리)
```

사용자 단위 락이므로 **서로 다른 사용자의 주문은 락 없이 병렬로 처리된다.**

#### OutboxEventPoller — 전역 락

```
락 키: lock:outbox:poller
waitTime: 0s  (다른 인스턴스가 처리 중이면 이번 사이클 건너뜀)
leaseTime: 30s (최대 50건 × 최대 500ms = 25초 → 여유 있게 30초)
```

```
[인스턴스 A] lock:outbox:poller 획득 → PENDING 50건 처리 중
[인스턴스 B] lock:outbox:poller 획득 실패 → 이번 사이클 건너뜀 (5초 후 재시도)
[인스턴스 C] lock:outbox:poller 획득 실패 → 이번 사이클 건너뜀
```

---

### 로컬 테스트 방법

#### 동일 사용자 동시 주문 테스트

```bash
# 같은 userId로 3건을 동시에 발송
for i in {1..3}; do
  curl -s -X POST http://localhost:8080/api/orders \
    -H "Content-Type: application/json" \
    -d '{"userId":"user-vip-1","totalAmount":59000}' &
done
wait

# 기대 결과:
# - 1건: {"orderId":1, "userId":"user-vip-1", ...}      ← 성공 (200 OK)
# - 2건: "이미 주문 처리 중입니다. 잠시 후 다시 시도해주세요."  ← 실패 (409 Conflict)
```

#### Redis 락 상태 직접 확인

```bash
# 처리 중에 락 키 확인 (leaseTime 5초 이내)
docker exec commerce-redis redis-cli KEYS "lock:*"

# OutboxEventPoller 락 확인
docker exec commerce-redis redis-cli GET "lock:outbox:poller"
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
│   ├── api/OrderController.java       # OrderFacade로 위임
│   ├── application/
│   │   ├── OrderService.java          # 순수 주문 생성 로직 (Redis 비의존)
│   │   └── OrderFacade.java           # 분산락 획득 후 OrderService 위임
│   ├── domain/
│   │   ├── Order.java
│   │   ├── OutboxEvent.java           # Outbox Pattern 엔티티
│   │   └── OutboxEventRepository.java
│   └── infrastructure/
│       ├── kafka/OrderEventPublisher.java
│       ├── outbox/OutboxEventPoller.java  # 전역 분산락 + PENDING 이벤트 Kafka 발행
│       └── redis/RedissonConfig.java  # RedissonClient 빈 설정
├── inventory-service/        (port: 8081)
│   └── infrastructure/kafka/
│       ├── OrderEventConsumer.java
│       ├── KafkaConsumerConfig.java   # DLQ ErrorHandler
│       └── DeadLetterConsumer.java    # DLT 소비자
├── notification-service/     (port: 8082)
│   └── infrastructure/kafka/
│       ├── OrderEventConsumer.java
│       ├── KafkaConsumerConfig.java
│       └── DeadLetterConsumer.java
└── product-service/          (port: 8083)
    ├── domain/Product.java, ProductRepository.java
    ├── application/
    │   ├── ProductService.java        # 상품 조회 + 랭킹 score 증가 연계
    │   └── RankingService.java        # Redis Sorted Set 랭킹 + Circuit Breaker
    ├── infrastructure/redis/
    │   └── RedisConfig.java           # RedisTemplate<String, String>
    └── resources/
        └── data.sql                   # 더미 상품 15개
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
# 별도 터미널에서 각각 실행
./gradlew :order-service:bootRun        # 1단계 주문 서비스
./gradlew :inventory-service:bootRun    # 1단계 재고 소비자
./gradlew :notification-service:bootRun # 1단계 알림 소비자
./gradlew :product-service:bootRun      # 2단계 상품 랭킹 서비스
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
- [x] **2단계**: Redis — 실시간 인기 상품 랭킹 (Sorted Set) + Circuit Breaker (Resilience4j)
- [x] **2단계 (계속)**: Redisson 분산락 — 주문 동시성 제어 (사용자별 락 + OutboxPoller 전역 락)
- [ ] **3단계**: Elasticsearch — 상품 검색 (MySQL LIKE → BM25)

---

## 참고 자료

- 패션 플랫폼 기업 기술 블로그
- [Apache Kafka 공식 문서](https://kafka.apache.org/documentation/)
- [Redisson 공식 문서](https://redisson.org/documentation.html)
