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
import org.checkerframework.checker.units.qual.A;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AtomicReference<WorkerHolder> workerHolder = new AtomicReference<>();
    private final int serverPort;
    private final int endpointsAllPort;
    private final JdbcWorkerInstanceService workerInstanceService;
    private final AbstractJdbcWorkerInstanceRepository workerInstanceRepository;
    private final ServerType serverType;
    private final ReentrantLock stateLock = new ReentrantLock();
    private final OnHeartbeatFailureCallback onHeartbeatFailureCallback;

    private final AtomicBoolean isWorkerStateUpdatable = new AtomicBoolean(true);

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
            new ExitOnHeartbeatFailureCallback()
        );
    }

    @VisibleForTesting
    JdbcWorkerLivenessHeartbeat(final WorkerHeartbeatLivenessConfig configuration,
                                final AbstractJdbcWorkerInstanceRepository workerInstanceRepository,
                                final JdbcWorkerInstanceService workerInstanceService,
                                int serverPort,
                                int endpointsAllPort,
                                final ServerType serverType,
                                final OnHeartbeatFailureCallback onHeartbeatFailureCallback) {
        super(TASK_NAME, configuration);
        this.workerInstanceRepository = Objects.requireNonNull(workerInstanceRepository, "workerInstanceRepository cannot be null");
        this.workerInstanceService = Objects.requireNonNull(workerInstanceService, "workerInstanceService cannot be null");
        this.onHeartbeatFailureCallback =  Objects.requireNonNull(onHeartbeatFailureCallback, "onHeartbeatFailureCallback cannot be null");
        this.serverPort = serverPort;
        this.endpointsAllPort = endpointsAllPort;
        this.serverType = serverType;
    }

    /**
     * Registers a new worker instance.
     *
     * @param event The worker event.
     */
    @EventListener
    public void onWorkerStateChangeEvent(final Worker.WorkerStateChangeEvent event) {
        if (!isWorkerStateUpdatable.get()) {
            WorkerInstance instance = getWorkerInstance();
            log.debug(
                "[Worker id={}, workerGroup={}, hostname={}] Worker state is not updatable. StateChangeEvent[{}] skipped.",
                instance.getWorkerUuid(),
                instance.getWorkerGroup(),
                instance.getHostname(),
                event.getState()
            );
            return;
        }

        WorkerInstance.Status newState = event.getState();
        switch (newState) {
            case RUNNING:
                onRunning(event);
                break;
            case TERMINATING, TERMINATED_GRACEFULLY, TERMINATED_FORCED:
                updateWorkerInstanceState(Instant.now(), newState, actualState -> {
                    if (actualState.hasCompletedShutdown()) {
                        WorkerInstance instance = getWorkerInstance();
                        log.error(
                            "[Worker id={}, workerGroup={}, hostname={}] Shutdown already completed ({}). " +
                             "This error may occur if the worker has already been evicted by a Kestra executor due to a prior error.",
                            instance.getWorkerUuid(),
                            instance.getWorkerGroup(),
                            instance.getHostname(),
                            actualState
                        );
                        this.isWorkerStateUpdatable.set(false);
                    }
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
        setWorkerInstance(event.getWorker(), this.workerInstanceRepository.save(instance));
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

        if (workerHolder.get() == null) {
            log.trace("Worker instance not registered yet. Skip scheduled heartbeat.");
            return;
        }

        // Proactively check whether this worker has timeout.
        if (ServerType.WORKER.equals(serverType) && isWorkerDisconnected(now)) {
            log.error("[Worker id={}, group='{}'] Failed to update state before reaching timeout ({}ms). Disconnecting.",
                getWorkerInstance().getWorkerUuid(),
                getWorkerInstance().getWorkerGroup(),
                getElapsedMilliSinceLastStateUpdate(now)
            );
            updateWorkerInstanceState(now, WorkerInstance.Status.DISCONNECTED, unused -> {
            });
            onHeartbeatFailure(WorkerInstance.Status.DISCONNECTED);
        }

        // Try to update the worker instance state.
        final long start = System.currentTimeMillis();
        updateWorkerInstanceState(now, getWorkerInstance().getStatus(), this::onHeartbeatFailure);
        log.trace("[Worker id={}, group='{}'] Completed worker heartbeat for state {} ({}ms).",
            getWorkerInstance().getWorkerUuid(),
            getWorkerInstance().getWorkerGroup(),
            getWorkerInstance().getStatus(),
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
        WorkerInstance currentWorkerInstance = getWorkerInstance();
        log.warn("[Worker id={}, group='{}'] Worker is : {}.",
            currentWorkerInstance.getWorkerUuid(),
            currentWorkerInstance.getWorkerGroup(),
            status
        );

        log.error("[Worker id={}, group='{}'] Terminating '{}' server.",
            serverType,
            currentWorkerInstance.getWorkerUuid(),
            currentWorkerInstance.getWorkerGroup()
        );
        this.onHeartbeatFailureCallback.execute(getWorker(), getWorkerInstance());
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
        if (workerHolder.get() == null) {
            return;
        }

        WorkerInstance localInstance = getWorkerInstance();
        // Pre-check the state transition validation with the known local state.
        if (!localInstance.getStatus().isValidTransition(newState)) {
            log.warn("Cannot transition worker [id={}, workerGroup={}, hostname={}] from {} to {}. Cause: {}.",
                localInstance.getWorkerUuid(),
                localInstance.getWorkerGroup(),
                localInstance.getHostname(),
                localInstance.getStatus(),
                newState,
                "Invalid transition"
            );
            onStateChangeError.accept(localInstance.getStatus());
        }

        // Ensure only one thread can the update the worker instance at a time.
        stateLock.lock();
        Runnable returnCallback = null;
        try {
            // try to update the worker state
            Optional<JdbcWorkerInstanceService.WorkerStateTransitionResponse> optional = workerInstanceService
                .mayTransitionWorkerTo(getWorkerInstance(), newState);

            if (optional.isEmpty()) {
                getWorkerInstance().setStatus(WorkerInstance.Status.EMPTY);
                returnCallback = () -> onStateChangeError.accept(WorkerInstance.Status.EMPTY);
                return;
            }

            JdbcWorkerInstanceService.WorkerStateTransitionResponse response = optional.get();

            // update the local worker instance
            setWorkerInstance(getWorker(), response.workerInstance());

            // check the transition result
            final WorkerStateTransitionResult result = response.result();
            if (result.equals(WorkerStateTransitionResult.INVALID)) {
                returnCallback = () -> onStateChangeError.accept(response.workerInstance().getStatus());
            }

            if (result.equals(WorkerStateTransitionResult.SUCCEED)) {
                this.lastSucceedStateUpdated = now;
            }
        } catch (Exception e) {
            log.error("[Worker id={}, group='{}'] Failed to update worker state. Error: {}",
                getWorkerInstance().getWorkerUuid(),
                getWorkerInstance().getWorkerGroup(),
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

    private Worker getWorker() {
        return workerHolder.get().worker();
    }

    /**
     * Returns the local registered worker instance.
     *
     * @return the {@link WorkerInstance}.
     */
    public WorkerInstance getWorkerInstance() {
        if (workerHolder.get() == null) {
            throw new IllegalStateException("No worker running"); // should not happen;
        }
        return workerHolder.get().instance();
    }

    @VisibleForTesting
    void setWorkerInstance(final Worker worker, final WorkerInstance instance) {
        Objects.requireNonNull(instance, "cannot set null worker instance.");
        this.workerHolder.set(new WorkerHolder(worker, instance));
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

    /**
     * Callback to be invoked on heartbeat failure.
     */
    @FunctionalInterface
    @VisibleForTesting
    interface OnHeartbeatFailureCallback {
        /**
         * The callback method.
         *
         * @param worker   the worker object.
         * @param instance the worker instance.
         */
        void execute(Worker worker, WorkerInstance instance);
    }

    private static class ExitOnHeartbeatFailureCallback implements OnHeartbeatFailureCallback {
        /**
         * {@inheritDoc}
         **/
        @Override
        public void execute(final Worker worker, final WorkerInstance instance) {
            WorkerInstance.Status status = instance.getStatus();
            // Skip graceful termination if the worker was already considered being not running.
            if (status.equals(WorkerInstance.Status.NOT_RUNNING) || status.equals(WorkerInstance.Status.EMPTY)) {
                worker.skipGracefulTermination(true);
            }
            Runtime.getRuntime().exit(1);
        }
    }

    private record WorkerHolder(Worker worker, WorkerInstance instance) {
    }
}