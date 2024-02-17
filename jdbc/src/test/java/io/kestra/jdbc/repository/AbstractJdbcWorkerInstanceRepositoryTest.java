package io.kestra.jdbc.repository;

import io.kestra.core.runners.WorkerInstance;
import io.kestra.core.utils.Network;
import io.kestra.jdbc.JdbcTestUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@MicronautTest(transactional = false)
public abstract class AbstractJdbcWorkerInstanceRepositoryTest {

    @Inject
    protected AbstractJdbcWorkerInstanceRepository workerInstanceRepository;

    @Inject
    JdbcTestUtils jdbcTestUtils;

    @BeforeEach
    protected void init() {
        jdbcTestUtils.drop();
        jdbcTestUtils.migrate();
    }

    @Test
    protected void save() {
        // Given
        WorkerInstance instance = Fixtures.RunningWorkerInstance;

        // When
        workerInstanceRepository.save(instance);

        // Then
        Optional<WorkerInstance> result = workerInstanceRepository.findByWorkerUuid(instance.getWorkerUuid().toString());
        Assertions.assertEquals(Optional.of(instance), result);
    }

    @Test
    void shouldDeleteAllWorkerInstancesInNotRunning() {
        // Given
        WorkerInstance instance = AbstractJdbcWorkerInstanceRepositoryTest.Fixtures.NotRunningWorkerInstance;
        workerInstanceRepository.save(instance);

        // When
        List<WorkerInstance> deleted = workerInstanceRepository.deleteAllWorkerInstancesInNotRunning();

        // Then
        Assertions.assertEquals(1, deleted.size());
        Assertions.assertTrue(workerInstanceRepository.findAll().isEmpty());
    }

    @Test
    protected void shouldDeleteGivenWorkerInstance() {
        // Given
        Fixtures.all().forEach(workerInstanceRepository::save);
        final WorkerInstance instance = Fixtures.DeadWorkerInstance;

        // When
        workerInstanceRepository.delete(instance);

        // Then
        Optional<WorkerInstance> result = workerInstanceRepository.findByWorkerUuid(instance.getWorkerUuid().toString());
        Assertions.assertEquals(Optional.empty(), result);
    }

    @Test
    protected void shouldFindByWorkerId() {
        // Given
        Fixtures.all().forEach(workerInstanceRepository::save);
        String uuid = Fixtures.DeadWorkerInstance.getWorkerUuid().toString();

        // When
        Optional<WorkerInstance> result = workerInstanceRepository.findByWorkerUuid(uuid);

        // Then
        Assertions.assertEquals(Optional.of(Fixtures.DeadWorkerInstance), result);
    }

    @Test
    protected void shouldFindAllWorkerInstances() {
        // Given
        Fixtures.all().forEach(workerInstanceRepository::save);

        // When
        List<WorkerInstance> results = workerInstanceRepository.findAll();

        // Then
        assertEquals(results.size(), Fixtures.all().size());
        assertThat(results, Matchers.containsInAnyOrder(Fixtures.all().toArray()));
    }

    @Test
    protected void shouldFindAllNonRunningInstances() {
        // Given
        Fixtures.all().forEach(workerInstanceRepository::save);

        // When
        List<WorkerInstance> results = workerInstanceRepository.findAllNonRunningInstances();

        // Then
        assertEquals(Fixtures.allNonRunning().size(), results.size());
        assertThat(results, Matchers.containsInAnyOrder(Fixtures.allNonRunning().toArray()));
    }

    @Test
    protected void shouldFindAllInstancesInNotRunningState() {
        // Given
        Fixtures.all().forEach(workerInstanceRepository::save);

        // When
        List<WorkerInstance> results = workerInstanceRepository.findAllInstancesInNotRunningState();

        // Then
        assertEquals(Fixtures.allInNotRunningState().size(), results.size());
        assertThat(results, Matchers.containsInAnyOrder(Fixtures.allInNotRunningState().toArray()));
    }

    @Test
    protected void shouldFindTimeoutRunningInstancesGivenTimeoutInstance() {
        // Given
        final Instant now = Instant.now();
        WorkerInstance instance = Fixtures.RunningWorkerInstance.toBuilder()
            .heartbeatDate(now.minus(Duration.ofSeconds(30)).truncatedTo(ChronoUnit.MILLIS))
            .build();
        workerInstanceRepository.save(instance);

        // When
        List<WorkerInstance> results = workerInstanceRepository.findAllTimeoutRunningInstances(
            now,
            Duration.ofSeconds(10)
        );

        // Then
        assertEquals(1, results.size());
        assertThat(results, Matchers.containsInAnyOrder(instance));
    }

    @Test
    protected void shouldNotFindTimeoutRunningInstanceGivenHealthyInstance() {
        // Given
        final Instant now = Instant.now();
        WorkerInstance instance = Fixtures.RunningWorkerInstance.toBuilder()
            .heartbeatDate(now.minus(Duration.ofSeconds(5)).truncatedTo(ChronoUnit.MILLIS))
            .build();
        workerInstanceRepository.save(instance);

        // When
        List<WorkerInstance> results = workerInstanceRepository.findAllTimeoutRunningInstances(
            now,
            Duration.ofSeconds(10)
        );

        // Then
        assertTrue(results.isEmpty());
    }

    public static final class Fixtures {

        public static List<WorkerInstance> all() {
            return List.of(
                UpWorkerInstance,
                RunningWorkerInstance,
                PendingShutdownWorkerInstance,
                GracefulShutdownWorkerInstance,
                ForcedShutdownWorkerInstance,
                DeadWorkerInstance,
                NotRunningWorkerInstance
            );
        }

        public static List<WorkerInstance> allNonRunning() {
            return List.of(
                PendingShutdownWorkerInstance,
                GracefulShutdownWorkerInstance,
                ForcedShutdownWorkerInstance,
                DeadWorkerInstance,
                NotRunningWorkerInstance
            );
        }

        public static List<WorkerInstance> allInNotRunningState() {
            return List.of(NotRunningWorkerInstance);
        }

        public static final WorkerInstance UpWorkerInstance =
            workerInstanceFor(WorkerInstance.Status.UP);

        public static final WorkerInstance RunningWorkerInstance =
            workerInstanceFor(WorkerInstance.Status.RUNNING);

        public static final WorkerInstance PendingShutdownWorkerInstance =
            workerInstanceFor(WorkerInstance.Status.TERMINATING);

        public static final WorkerInstance GracefulShutdownWorkerInstance =
            workerInstanceFor(WorkerInstance.Status.TERMINATED_GRACEFULLY);

        public static final WorkerInstance ForcedShutdownWorkerInstance =
            workerInstanceFor(WorkerInstance.Status.TERMINATED_FORCED);

        public static final WorkerInstance DeadWorkerInstance =
            workerInstanceFor(WorkerInstance.Status.DEAD);

        public static final WorkerInstance NotRunningWorkerInstance =
            workerInstanceFor(WorkerInstance.Status.NOT_RUNNING);

        public static WorkerInstance workerInstanceFor(final WorkerInstance.Status status) {
            return WorkerInstance.builder()
                .workerUuid(UUID.randomUUID())
                .workerGroup(null)
                .managementPort(0)
                .hostname(Network.localHostname())
                .partitions(Collections.emptyList())
                .heartbeatDate(Instant.now().truncatedTo(ChronoUnit.MILLIS))
                .startTime(Instant.now().truncatedTo(ChronoUnit.MILLIS))
                .port(0)
                .status(status)
                .build();
        }

    }
}