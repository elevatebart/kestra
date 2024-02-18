package io.kestra.jdbc.runner;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;

/**
 * Configuration for Liveness and Heartbeat mechanism between Workers and Executor.
 *
 * @param interval          The expected time between healthcheck (used by Executor).
 *
 * @param timeout           The timeout used to detect worker failures when using Kestra with JDBC deployment.
 *                          The worker sends periodic heartbeats to indicate its liveness to the executor.
 *                          If no heartbeats are received by the executor before the expiration of this session timeout,
 *                          then the executor will remove this worker from the cluster and eventually resubmit all its tasks.
 *
 * @param initialDelay      The time to wait before executing a liveness probe for a worker.
 * @param heartbeatInterval The expected time between worker heartbeats to the executor (used by workers).
 */
@ConfigurationProperties("kestra.worker.liveness")
public record WorkerHeartbeatLivenessConfig(
    @NotNull @Bindable(defaultValue = "true") Boolean enabled,
    @NotNull @Bindable(defaultValue = "5s") Duration interval,
    @NotNull @Bindable(defaultValue = "30s") Duration timeout,
    @NotNull @Bindable(defaultValue = "30s") Duration initialDelay,
    @NotNull @Bindable(defaultValue = "3s") Duration heartbeatInterval
) {
}
