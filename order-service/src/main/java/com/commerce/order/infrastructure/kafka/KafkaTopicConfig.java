package com.commerce.order.infrastructure.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    // 파티션 3개: 3개의 consumer가 병렬로 처리할 수 있는 최대 병렬도를 설정한다.
    // replication factor 1: 로컬 개발 환경에서는 브로커가 1개뿐이므로 1로 설정.
    // 운영 환경에서는 최소 3으로 설정하여 브로커 장애 시에도 데이터를 보존해야 한다.
    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name("order-events")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
