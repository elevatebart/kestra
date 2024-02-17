package io.kestra.jdbc.service;

import io.kestra.core.runners.WorkerInstance;
import io.kestra.jdbc.JdbcTestUtils;
import io.kestra.jdbc.repository.AbstractJdbcWorkerInstanceRepository;
import io.kestra.jdbc.repository.AbstractJdbcWorkerInstanceRepositoryTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

@MicronautTest(transactional = false)
abstract class JdbcWorkerInstanceServiceTest {

    @Inject
    protected JdbcWorkerInstanceService workerInstanceService;

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
    void shouldReturnEmptyForTransitionWorkerStateGivenInvalidWorker() {
        // Given
        WorkerInstance instance = AbstractJdbcWorkerInstanceRepositoryTest.Fixtures.RunningWorkerInstance;

        // When
        Optional<JdbcWorkerInstanceService.WorkerStateTransitionResponse> result = workerInstanceService
            .mayTransitWorkerTo(instance, WorkerInstance.Status.TERMINATING);

        // Then
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    void shouldReturnSucceedWorkerStateTransitionResponseForValidTransition() {
        // Given
        WorkerInstance instance = AbstractJdbcWorkerInstanceRepositoryTest.Fixtures.RunningWorkerInstance;
        workerInstanceRepository.save(instance);

        // When
        Optional<JdbcWorkerInstanceService.WorkerStateTransitionResponse> result = workerInstanceService
            .mayTransitWorkerTo(instance, WorkerInstance.Status.TERMINATING); // RUNNING -> PENDING_SHUTDOWN: valid transition

        // Then
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(JdbcWorkerInstanceService.WorkerStateTransitionResult.SUCCEED, result.get().result());
        Assertions.assertEquals(WorkerInstance.Status.TERMINATING, result.get().workerInstance().getStatus());
        Assertions.assertTrue(result.get().workerInstance().getHeartbeatDate().isAfter(instance.getHeartbeatDate()));
    }

    @Test
    void shouldReturnInvalidWorkerStateTransitionResponseForValidTransition() {
        // Given
        WorkerInstance instance = AbstractJdbcWorkerInstanceRepositoryTest.Fixtures.DeadWorkerInstance;
        workerInstanceRepository.save(instance);

        // When
        Optional<JdbcWorkerInstanceService.WorkerStateTransitionResponse> result = workerInstanceService
            .mayTransitWorkerTo(instance, WorkerInstance.Status.RUNNING); // DEAD -> RUNNING: INVALID transition

        // Then
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(JdbcWorkerInstanceService.WorkerStateTransitionResult.INVALID, result.get().result());
        Assertions.assertEquals(instance, result.get().workerInstance()); // instance was not updated
    }

}