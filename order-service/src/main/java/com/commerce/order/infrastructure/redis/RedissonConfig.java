package com.commerce.order.infrastructure.redis;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {

    @Value("${spring.data.redis.host}")
    private String host;

    @Value("${spring.data.redis.port}")
    private int port;

    // destroyMethod="shutdown": 애플리케이션 종료 시 RedissonClient 연결을 정상 해제한다.
    // 지정하지 않으면 애플리케이션이 종료될 때 Redis 연결이 남아있어 리소스가 누수될 수 있다.
    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient() {
        Config config = new Config();
        // useSingleServer: 단일 Redis 노드. 로컬 개발 환경에 적합.
        // 운영 환경에서는 useClusterServers()나 useSentinelServers()로
        // 고가용성을 확보하는 것이 일반적이다.
        config.useSingleServer()
              .setAddress("redis://" + host + ":" + port);
        return Redisson.create(config);
    }
}
