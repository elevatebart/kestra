package io.kestra.jdbc.runner;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

@MicronautTest
class WorkerHeartbeatLivenessConfigTest {

    @Inject
    WorkerHeartbeatLivenessConfig config;

    @Test
    void test() {
        Assertions.assertNotNull(config);
        Assertions.assertEquals(config.interval(), Duration.ofSeconds(5));
        Assertions.assertNotNull(config.initialDelay());
        Assertions.assertNotNull(config.timeout());
        Assertions.assertNotNull(config.heartbeatInterval());
    }
}