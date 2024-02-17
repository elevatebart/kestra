package io.kestra.jdbc.repository;

import io.kestra.core.repositories.WorkerInstanceRepositoryInterface;
import io.kestra.core.runners.WorkerInstance;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jooq.*;
import org.jooq.Record;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.jooq.impl.DSL.using;

@Singleton
@Getter
@Slf4j
public abstract class AbstractJdbcWorkerInstanceRepository extends AbstractJdbcRepository implements WorkerInstanceRepositoryInterface {

    private static final Field<Object> STATUS = field("status");
    private static final Field<Object> VALUE = field("value");
    private static final Field<Object> HEARTBEAT_DATE = field("heartbeat_date");
    private static final Field<Object> START_DATE = field("start_date");
    private static final Field<Object> WORKER_UUID = field("worker_uuid");

    protected io.kestra.jdbc.AbstractJdbcRepository<WorkerInstance> jdbcRepository;

    public AbstractJdbcWorkerInstanceRepository(final io.kestra.jdbc.AbstractJdbcRepository<WorkerInstance> jdbcRepository) {
        this.jdbcRepository = jdbcRepository;
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public Optional<WorkerInstance> findByWorkerUuid(final String workerUuid) {
        return jdbcRepository.getDslContextWrapper().transactionResult(
            configuration -> findByWorkerUuid(workerUuid, configuration, false)
        );
    }

    public Optional<WorkerInstance> findByWorkerUuid(final String workerUuid,
                                                     final Configuration configuration,
                                                     final boolean isForUpdate) {

        SelectConditionStep<Record1<Object>> query = using(configuration)
            .select(VALUE)
            .from(workerInstanceTable())
            .where(WORKER_UUID.eq(workerUuid));

        return isForUpdate ?
            this.jdbcRepository.fetchOne(query.forUpdate()) :
            this.jdbcRepository.fetchOne(query);
    }

    /**
     * Finds all running worker instances which have not sent a heartbeat for more than the given timeout, and thus
     * should be considered in failure.
     *
     * @param now     the time instance to be used for querying.
     * @param timeout the timeout used to detect worker failure.
     * @return the list of {@link WorkerInstance}.
     */
    public List<WorkerInstance> findAllTimeoutRunningInstances(final Instant now,
                                                               final Duration timeout) {
        return this.jdbcRepository.getDslContextWrapper()
            .transactionResult(configuration -> {
                SelectConditionStep<Record1<Object>> query = using(configuration)
                    .select(VALUE)
                    .from(workerInstanceTable())
                    .where(STATUS.in(WorkerInstance.Status.UP.name(), WorkerInstance.Status.RUNNING.name())
                    .and(HEARTBEAT_DATE.lessThan(now.minus(timeout))));
                return this.jdbcRepository.fetch(query);
            });
    }

    /**
     * Finds all worker instances which are in the given state.
     *
     * @return the list of {@link WorkerInstance}.
     */
    public List<WorkerInstance> findAllInstancesInState(final WorkerInstance.Status state) {
        return this.jdbcRepository.getDslContextWrapper()
            .transactionResult(configuration -> {
                SelectConditionStep<Record1<Object>> query = using(configuration)
                    .select(VALUE)
                    .from(workerInstanceTable())
                    .where(STATUS.eq(state.name()));
                return this.jdbcRepository.fetch(query);
            });
    }

    /**
     * Finds all worker instances which are NOT {@link WorkerInstance.Status#RUNNING}.
     *
     * @return the list of {@link WorkerInstance}.
     */
    public List<WorkerInstance> findAllNonRunningInstances() {
        return jdbcRepository.getDslContextWrapper().transactionResult(
            configuration -> findAllNonRunningInstances(configuration, false)
        );
    }

    /**
     * Finds all worker instances which are NOT {@link WorkerInstance.Status#RUNNING}.
     *
     * @return the list of {@link WorkerInstance}.
     */
    public List<WorkerInstance> findAllNonRunningInstances(final Configuration configuration,
                                                           final boolean isForUpdate) {
        SelectConditionStep<Record1<Object>> query = using(configuration)
            .select(VALUE)
            .from(workerInstanceTable())
            .where(STATUS.notIn(WorkerInstance.Status.UP.name(), WorkerInstance.Status.RUNNING.name()));

        return isForUpdate ?
            this.jdbcRepository.fetch(query.forUpdate()) :
            this.jdbcRepository.fetch(query);
    }

    /**
     * Finds all worker instances which are {@link WorkerInstance.Status#NOT_RUNNING}.
     *
     * @return the list of {@link WorkerInstance}.
     */
    public List<WorkerInstance> findAllInstancesInNotRunningState() {
        return jdbcRepository.getDslContextWrapper().transactionResult(
            configuration -> findAllInstancesInNotRunningState(configuration, false)
        );
    }

    /**
     * Finds all worker instances which are {@link WorkerInstance.Status#NOT_RUNNING}.
     *
     * @return the list of {@link WorkerInstance}.
     */
    public List<WorkerInstance> findAllInstancesInNotRunningState(final Configuration configuration,
                                                                  final boolean isForUpdate) {
        SelectConditionStep<Record1<Object>> query = using(configuration)
            .select(VALUE)
            .from(workerInstanceTable())
            .where(STATUS.eq(WorkerInstance.Status.NOT_RUNNING.name()));

        return isForUpdate ?
            this.jdbcRepository.fetch(query.forUpdate()) :
            this.jdbcRepository.fetch(query);
    }

    /**
     * Removes all worker instance which are in {@link WorkerInstance.Status#NOT_RUNNING}.
     */
    public List<WorkerInstance> deleteAllWorkerInstancesInNotRunning() {
        return transactionResult(configuration -> {
            DeleteResultStep<Record> delete = using(configuration)
                .deleteFrom(workerInstanceTable())
                .where(STATUS.eq(WorkerInstance.Status.NOT_RUNNING.name()))
                .returning(VALUE);
            return delete.fetch().map(this.jdbcRepository::map);
        });
    }

    public void transaction(final TransactionalRunnable runnable) {
        this.jdbcRepository
            .getDslContextWrapper()
            .transaction(runnable);
    }

    public <T> T transactionResult(final TransactionalCallable<T> runnable) {
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(runnable);
    }

    public void delete(DSLContext context, WorkerInstance workerInstance) {
        this.jdbcRepository.delete(context, workerInstance);
    }

    /** {@inheritDoc} **/
    @Override
    public void delete(WorkerInstance workerInstance) {
        this.jdbcRepository.delete(workerInstance);
    }

    /** {@inheritDoc} **/
    @Override
    public WorkerInstance save(WorkerInstance workerInstance) {
        this.jdbcRepository.persist(workerInstance, this.jdbcRepository.persistFields(workerInstance));
        return workerInstance;
    }

    /** {@inheritDoc} **/
    @Override
    public List<WorkerInstance> findAll() {
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(configuration -> this.jdbcRepository.fetch(
                using(configuration).select(VALUE).from(workerInstanceTable()))
            );
    }

    private Table<Record> workerInstanceTable() {
        return this.jdbcRepository.getTable();
    }
}
