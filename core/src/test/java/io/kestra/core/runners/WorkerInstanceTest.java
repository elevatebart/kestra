package io.kestra.core.runners;

import io.kestra.core.runners.WorkerInstance.Status;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class WorkerInstanceTest {

    @Test
    void shouldReturnValidTransitionForRunning() {
        List<Status> statuses = List.of(
            Status.RUNNING,
            Status.DISCONNECTED,
            Status.TERMINATING
        );
        statuses.forEach(status -> Assertions.assertTrue(Status.RUNNING.isValidTransition(status)));
    }

    @Test
    void shouldReturnValidTransitionForDisconnected() {
        List<Status> statuses = List.of(
            Status.DISCONNECTED,
            Status.TERMINATING,
            Status.NOT_RUNNING
        );
        statuses.forEach(status -> Assertions.assertTrue(Status.DISCONNECTED.isValidTransition(status)));
    }

    @Test
    void shouldReturnValidTransitionForPendingShutdown() {
        List<Status> statuses = List.of(
            Status.TERMINATING,
            Status.TERMINATED_FORCED,
            Status.TERMINATED_GRACEFULLY
        );
        statuses.forEach(status -> Assertions.assertTrue(Status.TERMINATING.isValidTransition(status)));
    }

    @Test
    void shouldReturnValidTransitionForForcedShutdown() {
        List<Status> statuses = List.of(
            Status.TERMINATED_FORCED,
            Status.NOT_RUNNING
        );
        statuses.forEach(status -> Assertions.assertTrue(Status.TERMINATED_FORCED.isValidTransition(status)));
    }

    @Test
    void shouldReturnValidTransitionForGracefulShutdown() {
        List<Status> statuses = List.of(
            Status.TERMINATED_GRACEFULLY,
            Status.NOT_RUNNING
        );
        statuses.forEach(status -> Assertions.assertTrue(Status.TERMINATED_GRACEFULLY.isValidTransition(status)));
    }

    @Test
    void shouldReturnValidTransitionForDead() {
        List<Status> statuses = List.of(
            Status.DEAD,
            Status.TERMINATING,
            Status.NOT_RUNNING
        );
        statuses.forEach(status -> Assertions.assertTrue(Status.DEAD.isValidTransition(status)));
    }

    @Test
    void shouldReturnValidTransitionForUp() {
        List<Status> statuses = List.of(
            Status.UP,
            Status.RUNNING,
            Status.DEAD,
            Status.TERMINATING
        );
        statuses.forEach(status -> Assertions.assertTrue(Status.UP.isValidTransition(status)));
    }

    @Test
    void shouldReturnTrueForDisconnectedOrPendingShutDown() {
            Assertions.assertTrue(Status.DISCONNECTED.isDisconnectedOrPendingShutDown());
            Assertions.assertTrue(Status.TERMINATING.isDisconnectedOrPendingShutDown());
            Assertions.assertTrue(Status.DEAD.isDisconnectedOrPendingShutDown());
    }
}