package io.kestra.jdbc.runner;

import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.runners.ServerInstance;
import io.kestra.core.runners.WorkerConfig;
import io.kestra.core.runners.WorkerInstance;
import io.kestra.jdbc.repository.AbstractJdbcWorkerInstanceRepository;
import io.kestra.jdbc.service.JdbcWorkerInstanceService;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * This class is responsible for managing the liveness of Worker instances.
 */
@Slf4j
@Singleton
@JdbcRunnerEnabled
@Requires(property = "kestra.server-type", pattern = "(EXECUTOR|STANDALONE)")
public final class JdbcWorkerLivenessHandler extends AbstractJdbcWorkerLivenessTask {

    private static final String TASK_NAME = "jdbc-worker-liveness-handler-task";

    private volatile JdbcExecutor executor;
    private final WorkerConfig workerConfig;
    private final JdbcWorkerInstanceService workerInstanceService;
    private final AbstractJdbcWorkerInstanceRepository workerInstanceRepository;
    private Instant lastScheduledExecution;
    private ServerInstance serverInstance = ServerInstance.getInstance();

    /**
     * Creates a new {@link JdbcWorkerLivenessHandler} instance.
     *
     * @param workerInstanceRepository The {@link AbstractJdbcWorkerInstanceRepository}.
     * @param workerConfig             The worker configuration.
     * @param livenessConfig           The worker liveness configuration.
     */
    @Inject
    public JdbcWorkerLivenessHandler(final JdbcWorkerInstanceService workerInstanceService,
                                     final AbstractJdbcWorkerInstanceRepository workerInstanceRepository,
                                     final WorkerConfig workerConfig,
                                     final WorkerHeartbeatLivenessConfig livenessConfig) {
        super(TASK_NAME, livenessConfig);
        this.workerConfig = workerConfig;
        this.workerInstanceService = workerInstanceService;
        this.workerInstanceRepository = workerInstanceRepository;
        this.lastScheduledExecution = Instant.now();
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    protected Duration getScheduleInterval() {
        return workerLivenessConfig.interval();
    }

    /**
     * {@inheritDoc}
     **/
    @Override

    protected void onSchedule(final Instant now, final boolean isLivenessEnabled) {
        if (executor == null) return; // only true during startup

        // (1) Detect and handle non-responding dead Workers.
        if (isLivenessEnabled) {
            final Instant minInstantForLivenessProbe = now.minus(workerLivenessConfig.initialDelay());
            List<WorkerInstance> nonRespondingWorkers = workerInstanceRepository
                // gets all non-responding workers (important: here only workers in a current state UP may be considered DEAD).
                .findAllTimeoutRunningInstances(now, workerLivenessConfig.timeout())
                .stream()
                // exclude any worker running on the same server as the executor, to prevent the latter from shuting down.
                .filter(instance -> !instance.getServer().equals(serverInstance))
                // only keep workers eligible for liveness probe
                .filter(instance -> instance.getStartTime().isBefore(minInstantForLivenessProbe))
                // warn
                .peek(instance -> log.warn("Detected non-responding worker [id={}, workerGroup={}, hostname={}] after timeout ({}ms).",
                    instance.getWorkerUuid(),
                    instance.getWorkerGroup(),
                    instance.getHostname(),
                    now.toEpochMilli() - instance.getHeartbeatDate().toEpochMilli()
                ))
                .toList();
            // (2) Attempt to transit all non-responding workers to DEAD.
            nonRespondingWorkers.forEach(instance -> {
                    // Handle backward compatibility with prior worker versions
                    WorkerInstance.Status nextStatus = instance.getStatus().equals(WorkerInstance.Status.UP) ?
                        WorkerInstance.Status.DEAD :
                        WorkerInstance.Status.DISCONNECTED;
                    workerInstanceService.safelyTransitWorkerTo(instance, nextStatus);
                }
            );
        }

        // (3) Finds all workers which are not in a RUNNING state.
        workerInstanceRepository.transaction(configuration -> {
            List<WorkerInstance> nonRunningWorkers = workerInstanceRepository
                .findAllNonRunningInstances(configuration, true);

            // (4) List of workers for which we don't know the actual state of tasks executions.
            final List<WorkerInstance> uncleanShutdownWorkers = new ArrayList<>();

            // ...all workers that have transitioned to DEAD or PENDING_SHUTDOWN for more than terminationGracePeriod).
            final Instant terminationGracePeriodStart = now.minus(workerConfig.terminationGracePeriod());
            uncleanShutdownWorkers.addAll(nonRunningWorkers.stream()
                .filter(nonRunning -> nonRunning.getStatus().isDisconnectedOrPendingShutDown())
                .filter(deadOrShuttingDown -> deadOrShuttingDown.getHeartbeatDate().isBefore(terminationGracePeriodStart))
                .peek(instance -> {
                    log.warn("Detected non-responding worker [id={}, workerGroup={}, hostname={}] after termination grace period ({}ms).",
                        instance.getWorkerUuid(),
                        instance.getWorkerGroup(),
                        instance.getHostname(),
                        now.toEpochMilli() - instance.getHeartbeatDate().toEpochMilli()
                    );
                })
                .toList()
            );
            // ...all workers that have transitioned to FORCED_SHUTDOWN.
            uncleanShutdownWorkers.addAll(nonRunningWorkers.stream()
                .filter(nonRunning -> nonRunning.getStatus().equals(WorkerInstance.Status.TERMINATED_FORCED))
                .toList()
            );

            // (5) Re-emit all WorkerJobs for unclean workers
            if (!uncleanShutdownWorkers.isEmpty()) {
                executor.reEmitWorkerJobsForWorkers(configuration, uncleanShutdownWorkers);
            }

            // (6) Transit all GRACEFUL AND UNCLEAN SHUTDOWN workers to NOT_RUNNING.
            Stream<WorkerInstance> cleanShutdownWorkers = nonRunningWorkers.stream()
                .filter(nonRunning -> nonRunning.getStatus().equals(WorkerInstance.Status.TERMINATED_GRACEFULLY));
            Stream.concat(cleanShutdownWorkers, uncleanShutdownWorkers.stream()).forEach(
                instance -> workerInstanceService.mayTransitWorkerTo(configuration, instance, WorkerInstance.Status.NOT_RUNNING)
            );
        });

        // (7) Remove all workers which are NOT_RUNNING anymore (i.e., we safely clean up the database).
        List<WorkerInstance> deleted = workerInstanceRepository.deleteAllWorkerInstancesInNotRunning();
        if (!deleted.isEmpty()) {
            log.info("Discarded '{}' workers in NOT_RUNNING state", deleted);
        }

        if (log.isInfoEnabled()) {
            // Log the newly-connected workers.
            workerInstanceRepository.findAllInstancesInState(WorkerInstance.Status.RUNNING)
                .stream()
                .filter(instance -> instance.getStartTime().isAfter(lastScheduledExecution))
                .forEach(instance -> {
                    log.info("Detected new worker [id={}, workerGroup={}, hostname={}] (started at: {}).",
                        instance.getWorkerUuid(),
                        instance.getWorkerGroup(),
                        instance.getHostname(),
                        instance.getStartTime()
                    );
                });
        }
        lastScheduledExecution = now;
    }

    void setExecutor(final JdbcExecutor executor) {
        this.executor = executor;
    }

    @VisibleForTesting
    void setServerInstance(final ServerInstance serverInstance) {
        this.serverInstance = serverInstance;
    }
}
