package com.commerce.product.application;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Service
public class RankingService {

    private static final Logger log = LoggerFactory.getLogger(RankingService.class);

    // [왜 MySQL이 아니라 Redis인가?]
    // MySQL로 조회수를 집계하면 매 조회마다 UPDATE products SET view_count = view_count + 1 이 발생한다.
    // 상품 조회가 초당 수천 건 이상일 때 이 UPDATE가 row-level lock을 잡고 경합을 일으켜 성능이 급격히 저하된다.
    // Redis의 ZINCRBY는 단일 명령으로 원자적 증가와 정렬을 동시에 처리하며,
    // 인메모리 연산이므로 MySQL 대비 수십~수백 배 빠르다.
    // 랭킹처럼 "정확한 영구 기록"보다 "빠른 실시간 반영"이 중요한 케이스에 Redis Sorted Set이 최적이다.

    // [ZINCRBY가 왜 동시성 문제가 없는가?]
    // Redis는 단일 스레드 이벤트 루프로 모든 명령을 순차 처리한다.
    // ZINCRBY는 "읽기 → 증가 → 쓰기"가 하나의 원자적 명령이기 때문에
    // 여러 서버 인스턴스에서 동시에 호출해도 중간에 끼어드는 명령이 없다.
    // MySQL UPDATE의 낙관적/비관적 락 없이도 카운터 정확성이 보장된다.

    // [Circuit Breaker가 필요한 이유 — 퀸잇 럭퀸세일 장애 사례]
    // 퀸잇의 럭퀸세일처럼 대규모 트래픽이 몰릴 때 Redis 응답이 지연되거나 다운되면,
    // 랭킹 API를 호출하는 스레드들이 모두 Redis 응답을 기다리며 타임아웃까지 블로킹된다.
    // 스레드 풀이 소진되면 다른 정상적인 API 요청도 처리할 수 없게 되어 전체 서비스가 다운된다.
    // 이것이 "장애 전파(Cascading Failure)"다.
    //
    // Circuit Breaker는 Redis 장애를 감지하면 더 이상 Redis에 요청을 보내지 않고
    // 즉시 fallback(빈 리스트)을 반환해 스레드를 점유하지 않는다.
    // 결과적으로 Redis 장애가 랭킹 API → 상품 서비스 전체 → 플랫폼 전체로 번지는 것을 차단한다.

    // [Circuit Breaker 세 가지 상태]
    // CLOSED (정상): 모든 요청을 Redis로 그대로 통과시킨다. 실패율을 슬라이딩 윈도우로 측정한다.
    //   → 실패율이 failureRateThreshold(50%) 초과 시 OPEN으로 전환.
    //
    // OPEN (차단): Redis가 불안정하다고 판단하여 모든 요청을 즉시 차단하고 fallback을 반환한다.
    //   실제로 Redis에 요청을 보내지 않으므로 스레드 블로킹이 없다.
    //   → waitDurationInOpenState(10s) 경과 후 HALF_OPEN으로 전환.
    //
    // HALF_OPEN (복구 탐색): 소수의 테스트 요청(permittedNumberOfCallsInHalfOpenState=3)만 허용해
    //   Redis가 실제로 복구됐는지 확인한다.
    //   → 성공률이 충분하면 CLOSED로, 실패하면 다시 OPEN으로 전환.

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    // [ranking:daily vs ranking:daily:20250525]
    // ranking:daily (고정 키): 구현이 단순하지만 TTL이 처음 키 생성 시점부터 시작되므로
    //   자정에 정확히 리셋되는 "일별 독립 랭킹"을 보장할 수 없다.
    //   예) 오전 6시에 키 생성 → 48시간 후인 다음날 오전 6시에 만료 → 날짜가 어긋남.
    //
    // ranking:daily:20250525 (날짜 포함 키): 날짜가 바뀌면 자연스럽게 새 키를 사용하므로
    //   각 날짜의 랭킹이 독립적으로 쌓인다. TTL은 집계 정확도가 아닌 메모리 정리 목적.
    //   이전 날 랭킹을 "어제 인기 상품"으로 별도 조회하는 것도 가능해진다.
    //   → 운영 환경에서는 날짜 포함 키가 더 안전하다.
    private String getRankingKey() {
        String today = LocalDate.now().format(DATE_FORMAT);
        return "ranking:daily:" + today;
    }

    // [TTL을 48시간으로 설정하는 이유]
    // 24시간으로 하면 날짜가 바뀌자마자 만료될 위험이 있다 (생성 시각 차이).
    // 48시간은 하루치 여유를 두어 자정 전후 경계에서 키가 사라지는 것을 방지하고,
    // 어제 랭킹 데이터를 당일 오전에도 참조 가능하게 한다.
    // 동시에 무한정 데이터가 쌓이지 않도록 Redis 메모리를 자동 정리한다.
    private static final Duration TTL = Duration.ofHours(48);

    private final RedisTemplate<String, String> redisTemplate;

    public RankingService(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // Redis 장애 시 상품 조회 API 자체가 실패하지 않도록 예외를 흡수한다.
    // 랭킹 점수 누락은 허용 가능한 손실이지만, 상품 조회 실패는 허용할 수 없다.
    public void incrementViewScore(Long productId) {
        try {
            String key = getRankingKey();
            redisTemplate.opsForZSet().incrementScore(key, String.valueOf(productId), 1);
            // TTL이 설정되지 않은 경우(-1)에만 expire를 걸어준다.
            // 매 조회마다 expire를 호출하면 TTL이 계속 리셋되므로,
            // 첫 조회(키 생성 시점)에만 TTL을 고정해 예측 가능한 만료 시각을 보장한다.
            Long currentTtl = redisTemplate.getExpire(key);
            if (currentTtl != null && currentTtl == -1L) {
                redisTemplate.expire(key, TTL);
            }
        } catch (Exception e) {
            log.warn("[RankingService] Redis 조회수 증가 실패 (productId={}): {}", productId, e.getMessage());
        }
    }

    @CircuitBreaker(name = "redisRanking", fallbackMethod = "getTopRankingFallback")
    public List<RankingItem> getTopRanking(int limit) {
        String key = getRankingKey();
        // ZREVRANGE: score 내림차순으로 0~(limit-1) 인덱스 범위 조회
        Set<ZSetOperations.TypedTuple<String>> tuples =
                redisTemplate.opsForZSet().reverseRangeWithScores(key, 0, limit - 1);

        List<RankingItem> result = new ArrayList<>();
        if (tuples != null) {
            for (ZSetOperations.TypedTuple<String> tuple : tuples) {
                result.add(new RankingItem(Long.parseLong(tuple.getValue()), tuple.getScore()));
            }
        }
        return result;
    }

    // Circuit Breaker가 OPEN 상태일 때(Redis 장애) 호출되는 fallback.
    // 빈 리스트를 반환해 랭킹 API는 200 OK를 유지하고 서비스 전파 장애를 차단한다.
    // fallback 메서드는 원본 메서드와 동일한 시그니처 + Throwable 파라미터를 가져야 한다.
    private List<RankingItem> getTopRankingFallback(int limit, Throwable t) {
        log.warn("[RankingService] Redis 장애로 인해 랭킹 fallback 반환: {}", t.getMessage());
        return Collections.emptyList();
    }

    public record RankingItem(Long productId, Double score) {}
}
