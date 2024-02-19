package io.kestra.core.runners;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Data
@SuperBuilder(toBuilder = true)
@NoArgsConstructor
public class WorkerInstance {
    @NotNull
    private UUID workerUuid;

    @NotNull
    private String hostname;
    private Integer port;
    private Integer managementPort;

    private String workerGroup;

    @Builder.Default
    private List<Integer> partitions = new ArrayList<>();

    @Builder.Default
    @JsonInclude
    private Status status = Status.RUNNING;

    @Builder.Default
    private Instant heartbeatDate = Instant.now();

    /**
     * The instant the worker instance started.
     */
    @Builder.Default
    private Instant startTime = Instant.now();

    /**
     * The Kestra server owning the worker.
     */
    @Builder.Default
    private ServerInstance server = ServerInstance.getInstance();

    /**
     * WorkerInstance status are the possible status that a Kestra's Worker instance can be in.
     * An instance must only be in one state at a time.
     * The expected state transition with the following defined states is:
     *
     * <pre>
     *                 +--------------+
     *         +<----- | Running      | -------->+
     *         |       +------+-------+          |
     *         |              |                  |
     *         |              v                  |
     *         |       +------+-------+     +-------+------+
     *         +-----&gt; | Terminating  |&lt;----| Disconnected |
     *                 +------+-------+     +-------+------+
     *                   |          |
     *                   v          v
     *      +------+-------+       +------+-------+
     *      | Terminated   |       | Terminated   |
     *      | Graceful     |       | Forced       |
     *      +--------------+       +--------------+
     *                    |         |
     *                    v         v
     *                  +------+-------+
     *                  | Not          |
     *                  | Running      |
     *                  +--------------+
     * </pre>
     */
    public enum Status {
        /**
         * @deprecated use RUNNING
         */
        @Deprecated
        UP(1, 2, 4),                    // 0

        RUNNING(3, 4),                  // 1
        /**
         * @deprecated use DISCONNECTED
         */
        @Deprecated
        DEAD (4, 7),                    // 2
        DISCONNECTED (4, 7),            // 3
        TERMINATING(5, 6, 7),      // 4
        TERMINATED_GRACEFULLY(7),           // 5
        TERMINATED_FORCED(7),             // 6
        NOT_RUNNING(),                                 // 7
        /**
         * Used to represent a non-existing worker instead of using null.
         */
        EMPTY();                                       // 8

        private final Set<Integer> validTransitions = new HashSet<>();

        Status(final Integer... validTransitions) {
            this.validTransitions.addAll(Arrays.asList(validTransitions));
        }

        public boolean isValidTransition(final Status newState) {
            return validTransitions.contains(newState.ordinal()) || equals(newState);
        }

        public boolean isDisconnectedOrPendingShutDown() {
            return equals(TERMINATING)
                || equals(DISCONNECTED)
                || equals(DEAD);
        }

        public boolean hasCompletedShutdown() {
            return equals(TERMINATED_GRACEFULLY)
                || equals(TERMINATED_FORCED)
                || equals(NOT_RUNNING)
                || equals(EMPTY);
        }
    }
}
