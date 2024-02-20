package io.kestra.jdbc.service;

import io.kestra.core.runners.WorkerInstance;
import io.kestra.jdbc.repository.AbstractJdbcWorkerInstanceRepository;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jooq.Configuration;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Singleton
public class JdbcWorkerInstanceService {

    private final AbstractJdbcWorkerInstanceRepository repository;

    @Inject
    public JdbcWorkerInstanceService(AbstractJdbcWorkerInstanceRepository repository) {
        this.repository = Objects.requireNonNull(repository, "repository cannot be null");
    }

    public void safelyTransitionWorkerTo(final WorkerInstance instance,
                                         final WorkerInstance.Status status) {
        try {
            mayTransitionWorkerTo(instance, status);
        } catch (Exception e) {
            // Log and ignore exception - it's safe to ignore error because the run() method is supposed to schedule at fix rate.
            log.error("Unexpected error while worker [id={}, workerGroup={}, hostname={}] transition from {} to {}. Error: {}",
                instance.getWorkerUuid(),
                instance.getWorkerGroup(),
                instance.getHostname(),
                instance.getStatus(),
                status,
                e.getMessage()
            );
        }
    }

    /**
     * Attempt to transition the status of a given worker to given new status.
     * This method may not update the worker if the transition is not valid.
     *
     * @param instance  the worker instance.
     * @param newStatus the new status of the worker.
     * @return an optional of the {@link WorkerInstance} or {@link Optional#empty()} if the worker is not running.
     */
    public Optional<WorkerStateTransitionResponse> mayTransitionWorkerTo(final WorkerInstance instance,
                                                                         final WorkerInstance.Status newStatus) {
        return this.repository.transactionResult(configuration -> mayTransitionWorkerTo(configuration, instance, newStatus));
    }

    /**
     * Attempt to transition the status of a given worker to given new status.
     * This method may not update the worker if the transition is not valid.
     *
     * @param instance  the worker instance.
     * @param newStatus the new status of the worker.
     * @return an optional of the {@link WorkerInstance} or {@link Optional#empty()} if the worker is not running.
     */
    public Optional<WorkerStateTransitionResponse> mayTransitionWorkerTo(final Configuration configuration,
                                                                         final WorkerInstance instance,
                                                                         final WorkerInstance.Status newStatus) {
        Optional<ImmutablePair<WorkerInstance, WorkerInstance>> optional = mayUpdateStatusById(
            configuration,
            instance.getWorkerUuid(),
            newStatus
        );
        if (optional.isEmpty()) {
            log.error("Failed to transition worker [id={}, workerGroup={}, hostname={}] to {}. Cause: {}",
                instance.getWorkerUuid(),
                instance.getWorkerGroup(),
                instance.getHostname(),
                newStatus,
                "Invalid worker."
            );
            return Optional.empty();
        }

        ImmutablePair<WorkerInstance, WorkerInstance> pair = optional.get();

        final WorkerInstance oldState = pair.getLeft();
        final WorkerInstance newState = pair.getRight();

        if (newState == null) {
            log.warn("Failed to transition worker [id={}, workerGroup={}, hostname={}] from {} to {}. Cause: {}.",
                instance.getWorkerUuid(),
                instance.getWorkerGroup(),
                instance.getHostname(),
                oldState.getStatus(),
                newStatus,
                "Invalid transition"
            );
            return Optional.of(new WorkerStateTransitionResponse(oldState, WorkerStateTransitionResult.INVALID));
        }

        // Logs if the state was changed, otherwise this method called for heartbeat purpose.
        if (!oldState.getStatus().equals(newState.getStatus())) {
            log.info("Worker [id={}, workerGroup={}, hostname={}] transition from {} to {}.",
                instance.getWorkerUuid(),
                instance.getWorkerGroup(),
                instance.getHostname(),
                oldState.getStatus(),
                newState.getStatus()
            );
        }
        return Optional.of(new WorkerStateTransitionResponse(newState, WorkerStateTransitionResult.SUCCEED));
    }

    /**
     * Attempt to transit the status of a given worker to given new status.
     * This method may not update the worker if the transition is not valid.
     *
     * @param id        the worker's uid.
     * @param newStatus the new status of the worker.
     * @return an {@link Optional} of {@link ImmutablePair} holding the old (left), and new {@link WorkerInstance} or {@code null} if transition failed (right).
     * Otherwise, an {@link Optional#empty()} if the no worker can be found.
     */
    private Optional<ImmutablePair<WorkerInstance, WorkerInstance>> mayUpdateStatusById(final Configuration configuration,
                                                                                        final UUID id,
                                                                                        final WorkerInstance.Status newStatus) {
        // Find the WorkerInstance to be updated
        Optional<WorkerInstance> optional = repository.findByWorkerUuid(
            id.toString(),
            configuration,
            true
        );
        // Check whether worker was found.
        if (optional.isEmpty()) {
            return Optional.empty();
        }

        // Check whether the status transition is valid before saving.
        WorkerInstance workerInstance = optional.get();
        if (workerInstance.getStatus().isValidTransition(newStatus)) {
            WorkerInstance updated = workerInstance
                .toBuilder()
                .status(newStatus)
                // it's OK to update heartbeat so that we used that field to track every Status changes.
                .heartbeatDate(Instant.now())
                .build();
            return Optional.of(new ImmutablePair<>(workerInstance, repository.save(updated)));
        }
        return Optional.of(new ImmutablePair<>(workerInstance, null));
    }

    public record WorkerStateTransitionResponse(WorkerInstance workerInstance, WorkerStateTransitionResult result) {
    }

    public enum WorkerStateTransitionResult {
        /**
         * Worker transition to new state successfully.
         */
        SUCCEED,
        /**
         * Worker failed to transition to new state due to invalid state transition.
         */
        INVALID
    }
}
