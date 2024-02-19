package io.kestra.jdbc.runner;

import io.kestra.core.models.ServerType;
import io.kestra.core.runners.Worker;
import io.kestra.core.runners.WorkerInstance;
import io.kestra.jdbc.repository.AbstractJdbcWorkerInstanceRepository;
import io.kestra.jdbc.service.JdbcWorkerInstanceService;
import io.kestra.jdbc.service.JdbcWorkerInstanceService.WorkerStateTransitionResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static io.kestra.core.runners.WorkerInstance.Status.*;
import static io.kestra.jdbc.service.JdbcWorkerInstanceService.WorkerStateTransitionResult.SUCCEED;

@ExtendWith(MockitoExtension.class)
class JdbcWorkerLivenessHeartbeatTest {

    private static final Duration DEFAULT_DURATION = Duration.ofSeconds(5);

    @Mock
    public AbstractJdbcWorkerInstanceRepository repository;

    @Mock
    public JdbcWorkerInstanceService service;

    @Captor
    ArgumentCaptor<WorkerInstance> workerInstanceCaptor;

    private JdbcWorkerLivenessHeartbeat workerLivenessHeartbeat;

    @Mock
    private Runnable onHeartBeatFailure;

    @BeforeEach
    void beforeEach() {
        this.workerLivenessHeartbeat = new JdbcWorkerLivenessHeartbeat(
            new WorkerHeartbeatLivenessConfig(true, Duration.ZERO, DEFAULT_DURATION, DEFAULT_DURATION, DEFAULT_DURATION),
            repository,
            service,
            0,
            0,
            ServerType.WORKER,
            onHeartBeatFailure
        );
    }

    @Test
    void shouldSaveWorkerInstanceOnRunningStateChange() {
        // Given
        final WorkerInstance instance = WorkerInstance
            .builder()
            .status(RUNNING)
            .build();

        final Worker.WorkerStateChangeEvent event = new Worker.WorkerStateChangeEvent(RUNNING, Mockito.mock(Worker.class));
       Mockito.when(repository.save(Mockito.any(WorkerInstance.class))).thenReturn(instance);

        // When
        workerLivenessHeartbeat.onWorkerStateChangeEvent(event);

        // Then
        Mockito.verify(repository, Mockito.only()).save(workerInstanceCaptor.capture());

        WorkerInstance value = workerInstanceCaptor.getValue();
        Assertions.assertEquals(RUNNING, value.getStatus());
        Assertions.assertEquals(instance, workerLivenessHeartbeat.getWorkerInstance());
    }

    @Test
    void shouldUpdateStateOnScheduleForSucceedTransition() {
        // Given
        final WorkerInstance instance = WorkerInstance
            .builder()
            .status(TERMINATING)
            .build();

        final WorkerStateTransitionResponse response = new WorkerStateTransitionResponse(
            instance,
            SUCCEED
        );

        Mockito
            .when(service.mayTransitWorkerTo(Mockito.any(WorkerInstance.class), Mockito.any(WorkerInstance.Status.class)))
            .thenReturn(Optional.of(response));

        workerLivenessHeartbeat.setWorkerInstance(WorkerInstance
            .builder()
            .status(RUNNING)
            .build());

        // When
        workerLivenessHeartbeat.onSchedule(Instant.now(), true);

        // Then
        Assertions.assertEquals(instance, workerLivenessHeartbeat.getWorkerInstance());
        Mockito.verify(onHeartBeatFailure, Mockito.never()).run();
    }

    @Test
    void shouldRunOnHeartbeatFailureForInvalidTransition() {
        // Given
        final WorkerInstance instance = WorkerInstance
            .builder()
            .status(DISCONNECTED)
            .build();

        final WorkerStateTransitionResponse response = new WorkerStateTransitionResponse(
            instance,
            JdbcWorkerInstanceService.WorkerStateTransitionResult.INVALID
        );

        Mockito
            .when(service.mayTransitWorkerTo(Mockito.any(WorkerInstance.class), Mockito.any(WorkerInstance.Status.class)))
            .thenReturn(Optional.of(response));

        workerLivenessHeartbeat.setWorkerInstance(WorkerInstance
            .builder()
            .status(RUNNING)
            .build());

        // When
        workerLivenessHeartbeat.onSchedule(Instant.now(), true);

        // Then
        Assertions.assertEquals(instance, workerLivenessHeartbeat.getWorkerInstance());
        Mockito.verify(onHeartBeatFailure, Mockito.only()).run();
    }

    @Test
    void shouldRunOnHeartbeatFailureForEmptyInstance() {
        // Given
        workerLivenessHeartbeat.setWorkerInstance(WorkerInstance
            .builder()
            .status(RUNNING)
            .build());
        Mockito
            .when(service.mayTransitWorkerTo(Mockito.any(WorkerInstance.class), Mockito.any(WorkerInstance.Status.class)))
            .thenReturn(Optional.empty());

        // When
        workerLivenessHeartbeat.onSchedule(Instant.now(), true);

        // Then
        Mockito.verify(onHeartBeatFailure, Mockito.only()).run();
    }

    @Test
    void shouldRunOnHeartbeatFailureForTimeout() {
        // Given
        final WorkerInstance instance = WorkerInstance
            .builder()
            .status(RUNNING)
            .build();
        workerLivenessHeartbeat.setWorkerInstance(instance);

        // When
        Instant now = Instant.now();
        final WorkerStateTransitionResponse response = new WorkerStateTransitionResponse(instance, SUCCEED);
        Mockito.when(service.mayTransitWorkerTo(Mockito.any(WorkerInstance.class), Mockito.any(WorkerInstance.Status.class))).thenReturn(Optional.of(response));
        workerLivenessHeartbeat.run(now); // SUCCEED
        Mockito.when(service.mayTransitWorkerTo(Mockito.any(WorkerInstance.class), Mockito.any(WorkerInstance.Status.class))).thenThrow(new RuntimeException());
        workerLivenessHeartbeat.run(now.plus(Duration.ofSeconds(2))); // FAIL
        Mockito.verify(onHeartBeatFailure, Mockito.never()).run();
        workerLivenessHeartbeat.run(now.plus(Duration.ofSeconds(4))); // FAIL
        Mockito.verify(onHeartBeatFailure, Mockito.never()).run();
        workerLivenessHeartbeat.run(now.plus(Duration.ofSeconds(6))); // TIMEOUT
        // Then
        Mockito.verify(onHeartBeatFailure, Mockito.only()).run();
    }

    @Test
    void shouldThrowIllegalStateExceptionWhenNoInstanceRegistered() {
        Assertions.assertThrows(IllegalStateException.class, () -> workerLivenessHeartbeat.getWorkerInstance());
    }
}