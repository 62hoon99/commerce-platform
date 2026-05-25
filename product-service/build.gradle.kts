dependencies {
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    // AOP: @CircuitBreaker 어노테이션이 프록시 기반으로 동작하기 위해 필요
    implementation("org.springframework.boot:spring-boot-starter-aop")
    // Resilience4j Spring Boot 3 스타터: Circuit Breaker, Retry 등 내결함성 패턴 제공
    implementation("io.github.resilience4j:resilience4j-spring-boot3:2.2.0")
    runtimeOnly("com.mysql:mysql-connector-j")
}
