package io.kestra.core.runners;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;

/**
 * Worker configuration
 *
 * @param terminationGracePeriod The expected time a worker to complete all of its
 *                               tasks before initiating a graceful shutdown.
 */
@ConfigurationProperties("kestra.worker")
public record WorkerConfig(
    @NotNull
    @Bindable(defaultValue = "5m")
    Duration terminationGracePeriod
) {
}
