package io.debezium.performance.dmt.async;

import io.debezium.performance.dmt.dao.DaoManager;
import io.quarkus.runtime.Startup;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
@Startup
public class ExecutorPool {
    private final ExecutorService pool;
    private final BlockingQueue<RunnableUpsert> runnableUpsertsQueue;

    private final BlockingQueue<RunnableBatchUpsert> runnableBatchUpsertsQueue;
    private CountDownLatch latch;
    private static final Logger LOG = Logger.getLogger(ExecutorPool.class);


    @Inject
    public ExecutorPool(@ConfigProperty(name = "executor.size", defaultValue = "10") int poolSize, DaoManager manager) {
        pool = Executors.newFixedThreadPool(poolSize);
        latch = new CountDownLatch(0);
        runnableUpsertsQueue = new ArrayBlockingQueue<>(poolSize);
        runnableBatchUpsertsQueue = new ArrayBlockingQueue<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            // For java insertion/update method change the Runnable.
            runnableUpsertsQueue.add(new RunnablePreparedUpsert(manager.getEnabledDbs()));
            runnableBatchUpsertsQueue.add(new RunnableBatchUpsert(manager.getEnabledDbs()));
        }
    }

    public void execute(String sqlQuery) {
        RunnableUpsert task;
        try {
            task = runnableUpsertsQueue.take();
        } catch (InterruptedException e) {
            return;
        }
//        LOG.info("Taken task");
        pool.submit(() -> {
            task.setSqlQuery(sqlQuery);
//            LOG.info("Set sqlQuery");
            task.run();
//            LOG.info("Finished task");
            try {
                runnableUpsertsQueue.put(task);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
//            LOG.info("Returned runnable");
        });
    }

    public void executeBatch(List<String> sqlQueries) {
        RunnableBatchUpsert task;
        try {
            task = runnableBatchUpsertsQueue.take();
        } catch (InterruptedException e) {
            return;
        }
        pool.submit(() -> {
            task.setSqlQueriesBatch(sqlQueries);
            task.run();
            try {
                runnableBatchUpsertsQueue.put(task);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
        });
    }

    public void setCountDownLatch(int number) {
        latch = new CountDownLatch(number);
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public int getPoolSize() {
        return ConfigProvider.getConfig().getValue("executor.size", int.class);
    }
}
