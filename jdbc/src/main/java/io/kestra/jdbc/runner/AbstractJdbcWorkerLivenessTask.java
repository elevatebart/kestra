package io.kestra.jdbc.runner;

import io.micronaut.core.annotation.Introspected;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for scheduling a task that operate on Worker liveness.
 */
@Introspected
@Slf4j
public abstract class AbstractJdbcWorkerLivenessTask implements Runnable, AutoCloseable {

    private final String name;
    protected final WorkerHeartbeatLivenessConfig workerLivenessConfig;
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private ScheduledExecutorService scheduledExecutorService;

    /**
     * Creates a new {@link AbstractJdbcWorkerLivenessTask} instance.
     *
     * @param name          the task name.
     * @param configuration the liveness configuration.
     */
    protected AbstractJdbcWorkerLivenessTask(final String name,
                                             final WorkerHeartbeatLivenessConfig configuration) {
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.workerLivenessConfig = Objects.requireNonNull(configuration, "configuration cannot be null");
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void run() {
        final Instant now = Instant.now();
        try {
            onSchedule(now, workerLivenessConfig.enabled());
        } catch (Exception e) {
            log.error("Unexpected error while executing '{}'. Error: {}", now, e.getMessage());
        }
    }

    /**
     * The callback method invoked on each schedule.
     *
     * @param now the time of the execution.
     * @throws Exception when something goes wrong during the execution.
     */
    protected abstract void onSchedule(final Instant now, boolean isLivenessEnabled) throws Exception;

    /**
     * Starts this task.
     */
    @PostConstruct
    public synchronized void start() {
        if (!workerLivenessConfig.enabled()) {
            log.warn(
                "Worker liveness is currently disabled (`worker.liveness.enabled=false`) " +
                "If you are running in production environment, please ensure this property is configured to `true`. "
            );
        }
        if (scheduledExecutorService == null && !isStopped.get()) {
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, name));
            Duration scheduleInterval = getScheduleInterval();
            log.debug("Scheduling '{}' at fixed rate {}.", name, scheduleInterval);
            scheduledExecutorService.scheduleAtFixedRate(
                this,
                scheduleInterval.toSeconds(),
                scheduleInterval.toSeconds(),
                TimeUnit.SECONDS
            );
        } else {
            throw new IllegalStateException(
                "The task '" + name + "' is either already started or already stopped, cannot re-start");
        }
    }

    /**
     * Returns the fixed rate duration for scheduling this task.
     *
     * @return a {@link Duration}.
     */
    protected abstract Duration getScheduleInterval();

    /**
     * Closes this task.
     */
    @PreDestroy
    @Override
    public void close() {
        if (isStopped.compareAndSet(false, true)) {
            if (scheduledExecutorService != null) {
                scheduledExecutorService.shutdown();
                log.debug("Stopped scheduled '{}' task.", name);
            }
        }
    }
}
