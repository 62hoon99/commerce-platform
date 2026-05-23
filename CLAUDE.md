# commerce-platform

패션 플랫폼 기업들의 기술 블로그를 참고하여 커머스 핵심 도메인을 직접 구현해보는 학습 · 포트폴리오 프로젝트.  
Kafka, Redis, Elasticsearch를 단계적으로 도입하며 각 기술의 도입 이유와 트레이드오프를 직접 체감하는 것이 목표.  
여러 패션 플랫폼 회사의 기술 블로그 내용을 접목하여 포트폴리오로 완성하는 것을 지향한다.

---

## 프로젝트 목표

- 패션 플랫폼 기업 기술 블로그의 실제 문제 해결 사례를 직접 구현하며 체득
- 강결합 구조의 문제를 이벤트 기반 아키텍처(Kafka)로 해결하는 경험
- Redis를 활용한 성능 최적화 및 분산 환경 동시성 제어 경험
- 각 기술 도입의 "왜"를 설명할 수 있는 수준의 이해

---

## 기술 스택

| 분류 | 기술 |
|------|------|
| Language | Java |
| Framework | Spring Boot 3.x |
| Message Broker | Apache Kafka |
| Cache / Lock | Redis (Redisson) |
| Database | MySQL 8.x |
| Search | Elasticsearch (3단계 예정) |
| Infrastructure | Docker Compose (로컬 전체 환경) |
| Build | Gradle (Kotlin DSL) |

---

## 프로젝트 구조

```
commerce-platform/
├── CLAUDE.md
├── docker-compose.yml
├── build.gradle.kts          # 루트 빌드 설정 (Kotlin DSL)
├── settings.gradle.kts       # 멀티모듈 설정
├── order-service/            # 주문 생성, Kafka 이벤트 발행
├── inventory-service/        # 재고 감소 Kafka 컨슈머
└── notification-service/     # 알림 Kafka 컨슈머
```

---

## 도입 단계 로드맵

### 1단계: Kafka — 이벤트 기반 주문 파이프라인
- 주문 생성 → `order-events` 토픽 발행
- inventory-service / notification-service: Kafka 소비자
- Outbox Pattern: 주문 저장과 이벤트 발행의 원자성 보장
- Dead Letter Queue: 소비자 처리 실패 이벤트 재처리
- **학습 목표**: 강결합 vs 이벤트 기반 구조 차이 체감, 운영 신뢰성 패턴

### 2단계: Redis — 성능 최적화 및 동시성 제어
- Sorted Set 기반 실시간 인기 상품 랭킹
- Redisson 분산락으로 주문 동시성 제어
- Circuit Breaker + Redis Fallback (Resilience4j)

### 3단계: Elasticsearch — 상품 검색
- MySQL LIKE → Elasticsearch BM25 전환

---

## 개발 원칙

- **왜를 먼저**: 기술을 도입할 때 반드시 "왜 이 기술인가"를 README나 주석에 먼저 작성
- **단계별 완성**: 각 단계를 완전히 이해하고 글로 설명할 수 있을 때 다음 단계로 이동
- **트레이드오프 기록**: 설계 결정마다 다른 선택지와 비교한 내용을 남김
- **포스팅 연계**: 각 단계 완료 후 기술 블로그 포스팅 작성 (학습 내용 + 삽질 기록)

---

## 코드 컨벤션

- Java 공식 코딩 컨벤션 준수
- 패키지 구조: `com.commerce.{서비스명}.{도메인}`
- 도메인 레이어는 외부 기술(Kafka, Redis 등)에 의존하지 않도록 분리
- 각 서비스는 독립적으로 실행 가능해야 함

---

## 로컬 실행

```bash
# 인프라 전체 실행
docker-compose up -d

# 개별 서비스 실행 (루트에서)
./gradlew :order-service:bootRun
./gradlew :inventory-service:bootRun
./gradlew :notification-service:bootRun
```

---

## 참고 자료

- 패션 플랫폼 기업 기술 블로그
- [Apache Kafka 공식 문서](https://kafka.apache.org/documentation/)
- [Redisson 공식 문서](https://redisson.org/documentation.html)
