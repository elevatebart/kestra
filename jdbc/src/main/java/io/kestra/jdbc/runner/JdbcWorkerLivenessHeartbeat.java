package io.kestra.jdbc.runner;

import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.models.ServerType;
import io.kestra.core.runners.Worker;
import io.kestra.core.runners.WorkerInstance;
import io.kestra.core.utils.Network;
import io.kestra.jdbc.repository.AbstractJdbcWorkerInstanceRepository;
import io.kestra.jdbc.service.JdbcWorkerInstanceService;
import io.kestra.jdbc.service.JdbcWorkerInstanceService.WorkerStateTransitionResult;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.runtime.event.annotation.EventListener;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * This class is responsible for sending periodic heartbeats to indicate worker's liveness.
 */
@Context
@JdbcRunnerEnabled
@Requires(property = "kestra.server-type", pattern = "(WORKER|STANDALONE)")
@Slf4j
public final class JdbcWorkerLivenessHeartbeat extends AbstractJdbcWorkerLivenessTask {

    private static final String TASK_NAME = "jdbc-worker-liveness-heartbeat-task";
    private final AtomicReference<WorkerInstance> workerInstance = new AtomicReference<>();
    private final int serverPort;
    private final int endpointsAllPort;
    private final JdbcWorkerInstanceService workerInstanceService;
    private final AbstractJdbcWorkerInstanceRepository workerInstanceRepository;
    private final ServerType serverType;
    private final ReentrantLock stateLock = new ReentrantLock();
    private final Runnable onHeartbeatFailure;

    private Instant lastSucceedStateUpdated;

    @Inject
    public JdbcWorkerLivenessHeartbeat(final WorkerHeartbeatLivenessConfig configuration,
                                       final AbstractJdbcWorkerInstanceRepository workerInstanceRepository,
                                       final JdbcWorkerInstanceService workerInstanceService,
                                       @Value("${micronaut.server.port:8080}") int serverPort,
                                       @Value("${endpoints.all.port:8081}") int endpointsAllPort,
                                       @Value("${kestra.server-type}") ServerType serverType) {
        this(
            configuration,
            workerInstanceRepository,
            workerInstanceService,
            serverPort,
            endpointsAllPort,
            serverType,
            // TODO implement a graceful and clean way to shutdown the application.
            () -> Runtime.getRuntime().exit(1)
        );
    }

    @VisibleForTesting
    JdbcWorkerLivenessHeartbeat(final WorkerHeartbeatLivenessConfig configuration,
                                final AbstractJdbcWorkerInstanceRepository workerInstanceRepository,
                                final JdbcWorkerInstanceService workerInstanceService,
                                int serverPort,
                                int endpointsAllPort,
                                ServerType serverType,
                                Runnable onHeartbeatFailure) {
        super(TASK_NAME, configuration);
        this.serverPort = serverPort;
        this.endpointsAllPort = endpointsAllPort;
        this.workerInstanceRepository = workerInstanceRepository;
        this.workerInstanceService = workerInstanceService;
        this.serverType = serverType;
        this.onHeartbeatFailure = onHeartbeatFailure;
    }

    /**
     * Registers a new worker instance.
     *
     * @param event The worker event.
     */
    @EventListener
    public void onWorkerStateChangeEvent(final Worker.WorkerStateChangeEvent event) {
        WorkerInstance.Status newState = event.getState();
        switch (newState) {
            case RUNNING:
                onRunning(event);
                break;
            case TERMINATING, TERMINATED_GRACEFULLY, TERMINATED_FORCED:
                updateWorkerInstanceState(Instant.now(), newState, actualState -> {
                    if (actualState.hasCompletedShutdown()) {
                        WorkerInstance instance = workerInstance.get();
                        log.error(
                            "[Worker id={}, workerGroup={}, hostname={}] Shutdown already completed ({}). " +
                                "This error may occur if the worker has already been evicted by a Kestra executor due to a prior error.",
                            instance.getWorkerUuid(),
                            instance.getWorkerGroup(),
                            instance.getHostname(),
                            actualState
                        );
                    }
                    log.warn("Failed to ");
                });
                break;
            default:
                log.warn("Unsupported worker state: {}. Ignored.", event.getState());
        }
    }

    /**
     * Handles {@link WorkerInstance.Status#RUNNING}.
     */
    private void onRunning(final Worker.WorkerStateChangeEvent event) {
        WorkerInstance instance = newWorkerInstance(event.getWorker());
        setWorkerInstance(this.workerInstanceRepository.save(instance));
        log.info("[Worker id={}, group='{}'] Connected",
            instance.getWorkerUuid(),
            instance.getWorkerGroup()
        );
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    protected Duration getScheduleInterval() {
        return workerLivenessConfig.heartbeatInterval();
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    protected void onSchedule(final Instant now, final boolean isLivenessEnabled) {
        if (!isLivenessEnabled) return; // Heartbeat is disabled

        if (workerInstance.get() == null) {
            log.trace("Worker instance not registered yet. Skip scheduled heartbeat.");
            return;
        }

        // Proactively check whether this worker has timeout.
        if (serverType.equals(ServerType.WORKER) && isWorkerDisconnected(now)) {
            log.error("[Worker id={}, group='{}'] Failed to update state before reaching timeout ({}ms). Disconnecting.",
                workerInstance.get().getWorkerUuid(),
                workerInstance.get().getWorkerGroup(),
                getElapsedMilliSinceLastStateUpdate(now)
            );
            updateWorkerInstanceState(now, WorkerInstance.Status.DISCONNECTED, unused -> {
            });
            onHeartbeatFailure(WorkerInstance.Status.DISCONNECTED);
        }

        // Try to update the worker instance state.
        final long start = System.currentTimeMillis();
        updateWorkerInstanceState(now, workerInstance.get().getStatus(), this::onHeartbeatFailure);
        log.trace("[Worker id={}, group='{}'] Completed worker heartbeat for state {} ({}ms).",
            workerInstance.get().getWorkerUuid(),
            workerInstance.get().getWorkerGroup(),
            workerInstance.get().getStatus(),
            System.currentTimeMillis() - start
        );
    }

    private boolean isWorkerDisconnected(final Instant now) {
        long timeoutMilli = workerLivenessConfig.timeout().toMillis();
        return getElapsedMilliSinceLastSchedule(now) < timeoutMilli && // check whether the JVM was not frozen
            getElapsedMilliSinceLastStateUpdate(now) > timeoutMilli;
    }

    private long getElapsedMilliSinceLastSchedule(final Instant now) {
        return now.toEpochMilli() - lastScheduledExecution().toEpochMilli();
    }

    private long getElapsedMilliSinceLastStateUpdate(final Instant now) {
        return now.toEpochMilli() - (lastSucceedStateUpdated != null ? lastSucceedStateUpdated.toEpochMilli() : now.toEpochMilli());
    }

    private void onHeartbeatFailure(final WorkerInstance.Status status) {
        WorkerInstance currentWorkerInstance = workerInstance.get();
        log.warn("[Worker id={}, group='{}'] Worker was transition to : {}.",
            currentWorkerInstance.getWorkerUuid(),
            currentWorkerInstance.getWorkerGroup(),
            status
        );

        log.error("[Worker id={}, group='{}'] Shutting down server.",
            currentWorkerInstance.getWorkerUuid(),
            currentWorkerInstance.getWorkerGroup()
        );
        this.onHeartbeatFailure.run();
    }

    /**
     * Update the state of the local worker instance.
     *
     * @param newState           the new worker state.
     * @param onStateChangeError the callback to invoke if the state cannot be changed.
     */
    private void updateWorkerInstanceState(final Instant now,
                                           final WorkerInstance.Status newState,
                                           final Consumer<WorkerInstance.Status> onStateChangeError) {
        // Check whether a local worker was already registered
        if (workerInstance.get() == null) {
            return;
        }

        WorkerInstance localInstance = workerInstance.get();
        // Pre-check the state transition validation with the known local state.
        if (!localInstance.getStatus().isValidTransition(newState)) {
            log.warn("Failed to transition worker [id={}, workerGroup={}, hostname={}] from {} to {}. Cause: {}.",
                localInstance.getWorkerUuid(),
                localInstance.getWorkerGroup(),
                localInstance.getHostname(),
                localInstance.getStatus(),
                newState,
                "Invalid transition"
            );
            return;
        }

        // Ensure only one thread can the update the worker instance at a time.
        stateLock.lock();
        Runnable returnCallback = null;
        try {
            Optional<JdbcWorkerInstanceService.WorkerStateTransitionResponse> optional = workerInstanceService
                .mayTransitWorkerTo(workerInstance.get(), newState);

            if (optional.isEmpty()) {
                returnCallback = () -> onStateChangeError.accept(WorkerInstance.Status.EMPTY);
                return;
            }

            JdbcWorkerInstanceService.WorkerStateTransitionResponse response = optional.get();
            WorkerInstance instance = response.workerInstance();
            setWorkerInstance(instance);
            if (response.result().equals(WorkerStateTransitionResult.INVALID)) {
                returnCallback = () -> onStateChangeError.accept(instance.getStatus());
            }

            if (response.result().equals(WorkerStateTransitionResult.SUCCEED)) {
                this.lastSucceedStateUpdated = now;
            }

        } catch (Exception e) {
            log.error("[Worker id={}, group='{}'] Failed to update worker state. Error: {}",
                workerInstance.get().getWorkerUuid(),
                workerInstance.get().getWorkerGroup(),
                e.getMessage()
            );
        } finally {
            stateLock.unlock();
            // Because the callback may trigger a new thread that will update
            // the worker instance we must ensure that we run it after calling unlock.
            if (returnCallback != null) {
                returnCallback.run();
            }
        }
    }

    /**
     * Returns the local registered worker instance.
     *
     * @return the {@link WorkerInstance}.
     */
    public WorkerInstance getWorkerInstance() {
        if (workerInstance.get() == null) {
            throw new IllegalStateException("No worker running"); // should not happen;
        }
        return workerInstance.get();
    }

    @VisibleForTesting
    void setWorkerInstance(final WorkerInstance instance) {
        this.workerInstance.set(Objects.requireNonNull(instance, "cannot set null worker instance."));
    }

    private WorkerInstance newWorkerInstance(final Worker worker) {
        return WorkerInstance.builder()
            .workerUuid(UUID.randomUUID())
            .hostname(Network.localHostname())
            .port(serverPort)
            .managementPort(endpointsAllPort)
            .workerGroup(worker.getWorkerGroup())
            .build();
    }
}