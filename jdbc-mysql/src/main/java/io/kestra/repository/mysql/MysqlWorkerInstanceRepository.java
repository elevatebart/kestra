package io.kestra.repository.mysql;

import io.kestra.core.runners.WorkerInstance;
import io.kestra.jdbc.repository.AbstractJdbcWorkerInstanceRepository;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jooq.DeleteResultStep;
import org.jooq.Record;

import java.util.List;

import static org.jooq.impl.DSL.using;

@Singleton
@MysqlRepositoryEnabled
public class MysqlWorkerInstanceRepository extends AbstractJdbcWorkerInstanceRepository {
    @Inject
    public MysqlWorkerInstanceRepository(ApplicationContext applicationContext) {
        super(new MysqlRepository<>(WorkerInstance.class, applicationContext));
    }

    /**
     * Removes all worker instance which are in {@link WorkerInstance.Status#NOT_RUNNING}.
     */
    @Override
    public List<WorkerInstance> deleteAllWorkerInstancesInNotRunning() {
        return transactionResult(configuration -> {
            List<WorkerInstance> instances =
                findAllInstancesInNotRunningState(configuration, true);
            instances.forEach(this::delete);
            return instances;
        });
    }
}
