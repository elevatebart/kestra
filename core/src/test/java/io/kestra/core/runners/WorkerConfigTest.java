package io.kestra.core.runners;

import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

@MicronautTest
@Property(name = "kestra.worker.terminationGracePeriod", value = "5s")
class WorkerConfigTest {

    @Inject
    WorkerConfig config;

    @Test
    void test() {
        Assertions.assertNotNull(config);
        Assertions.assertEquals(config.terminationGracePeriod(), Duration.ofSeconds(5));
    }
}