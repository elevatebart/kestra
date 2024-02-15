package io.kestra.jdbc.runner;

import io.kestra.core.runners.Worker;
import io.kestra.core.runners.WorkerInstance;
import io.kestra.core.utils.Await;
import io.kestra.jdbc.repository.AbstractJdbcWorkerInstanceRepository;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.UUID;

@Singleton
@JdbcRunnerEnabled
@Requires(property = "kestra.server-type", pattern = "(WORKER|STANDALONE)")
@Slf4j
public class JdbcHeartbeat {
    private static final String HOSTNAME;

    static {
        try {
            HOSTNAME = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private final AbstractJdbcWorkerInstanceRepository repository;
    private volatile WorkerInstance workerInstance;
    private final int serverPort;
    private final int endpointsAllPort;

    @Inject
    public JdbcHeartbeat(AbstractJdbcWorkerInstanceRepository repository,
                         @Value("${micronaut.server.port:8080}") int serverPort,
                         @Value("${endpoints.all.port:8081}") int endpointsAllPort) {
        this.repository = repository;
        this.serverPort = serverPort;
        this.endpointsAllPort = endpointsAllPort;
    }

    /**
     * Registers a new worker instance.
     *
     * @param event The worker event.
     */
    @EventListener
    public void registerWorker(final Worker.StartupEvent event) {
        synchronized (this) {
            this.workerInstance = joinCluster(newWorkerInstance(event.getWorker()));
        }
    }

    /**
     * Unregisters a worker instance.
     *
     * @param event The worker event.
     */
    @EventListener
    public void unregisterWorker(final Worker.ShutdownEvent event) {
        if (workerInstance != null) {
            synchronized (this) {
                log.info("[Worker id={}, group='{}'] Leaving cluster.",
                    workerInstance.getWorkerUuid(),
                    workerInstance.getWorkerGroup()
                );
                repository.delete(workerInstance);
                workerInstance = null;
            }
        } else {
            log.warn("Failed to unregister worker. No worker instance.");
        }
    }

    private WorkerInstance newWorkerInstance(final Worker worker) {
        return WorkerInstance.builder()
            .workerUuid(UUID.randomUUID())
            .hostname(HOSTNAME)
            .port(serverPort)
            .managementPort(endpointsAllPort)
            .workerGroup(worker.getWorkerGroup())
            .build();
    }

    @Scheduled(fixedDelay = "${kestra.heartbeat.frequency}")
    public void updateHeartbeat() {
        if (this.workerInstance == null) {
            // The worker is either not yet registered or has already unregistered itself.
            // In both cases, skip heartbeat
            log.debug("No worker instance registered. Skip heartbeat.");
            return;
        }
        synchronized (this) {

            final long start = System.currentTimeMillis();
            this.workerInstance = heartbeatAndGet(this.workerInstance);
            log.trace("[Worker id={}, group='{}'] Completed heartbeat ({}ms).",
                workerInstance.getWorkerUuid(),
                workerInstance.getWorkerGroup(),
                System.currentTimeMillis() - start
            );
        }
    }

    private WorkerInstance heartbeatAndGet(final WorkerInstance workerInstance) {
        log.trace("[Worker id={}, group='{}'] Sending heartbeat.",
            workerInstance.getWorkerUuid(),
            workerInstance.getWorkerGroup()
        );
        try {
            final String uuid = workerInstance.getWorkerUuid().toString();

            Optional<WorkerInstance> result = repository.heartbeatCheckUp(uuid);
            if (result.isPresent()) {
                return result.get();
            }

            // Let's get current state to provide meaningful log
            result = repository.findByWorkerUuid(uuid);

            WorkerInstance.Status status = result.isEmpty() ?
                WorkerInstance.Status.EMPTY :
                result.get().getStatus();
            log.warn("[Worker id={}, group='{}'] Received heartbeat response error. Worker was stated to : {}.",
                workerInstance.getWorkerUuid(),
                workerInstance.getWorkerGroup(),
                status
            );

            log.error("[Worker id={}, group='{}'] Shutting down server.",
                workerInstance.getWorkerUuid(),
                workerInstance.getWorkerGroup()
            );
            // TODO implement a graceful and clean way to shutdown the application.
            Runtime.getRuntime().exit(1);
        } catch (Exception e) {
            log.error("[Worker id={}, group='{}'] Failed to send heartbeat. Error: {}",
                workerInstance.getWorkerUuid(),
                workerInstance.getWorkerGroup(),
                e.getLocalizedMessage()
            );
        }
        return workerInstance;
    }

    private WorkerInstance joinCluster(final WorkerInstance instance) {
        log.info("[Worker id={}, group='{}'] Joining cluster",
            instance.getWorkerUuid(),
            instance.getWorkerGroup()
        );
        return this.repository.save(instance);
    }

    public WorkerInstance getWorkerInstance() {
        if (workerInstance == null) {
            Await.until(() -> workerInstance != null);
        }
        return workerInstance;
    }
}