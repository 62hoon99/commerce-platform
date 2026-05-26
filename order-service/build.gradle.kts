dependencies {
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    // Redisson: Redis 기반 분산락 구현체.
    // redisson-spring-boot-starter 대신 redisson만 사용해 직접 설정하는 방식을 택한다.
    // 설정이 명시적으로 드러나도록 하기 위함.
    implementation("org.redisson:redisson:3.32.0")
    runtimeOnly("com.mysql:mysql-connector-j")
}
