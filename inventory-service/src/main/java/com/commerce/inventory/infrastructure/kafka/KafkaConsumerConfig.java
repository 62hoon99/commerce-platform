package com.commerce.inventory.infrastructure.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

// Dead Letter Queue 설정.
// DefaultErrorHandler를 @Bean으로 등록하면 Spring Boot가 자동으로
// KafkaListenerContainerFactory에 적용한다.
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // 처리 실패 시 지수 백오프로 최대 3회 재시도 후 order-events.DLT 토픽으로 라우팅한다.
    // 지수 백오프(1초 → 2초 → 4초): 일시적인 네트워크 오류나 DB 부하를 자연스럽게 흡수한다.
    // 단순 고정 간격보다 백엔드 시스템에 가해지는 재시도 부하가 훨씬 작다.
    @Bean
    public DefaultErrorHandler errorHandler() {
        // DLT 발행에는 ByteArraySerializer를 사용한다.
        // DeadLetterPublishingRecoverer는 원본 ConsumerRecord의 raw bytes를 그대로 DLT에 전송하므로
        // JsonSerializer가 아닌 ByteArraySerializer가 필요하다.
        KafkaTemplate<String, byte[]> dltTemplate = createDltKafkaTemplate();
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dltTemplate);

        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxAttempts(3);

        return new DefaultErrorHandler(recoverer, backOff);
    }

    private KafkaTemplate<String, byte[]> createDltKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }
}
