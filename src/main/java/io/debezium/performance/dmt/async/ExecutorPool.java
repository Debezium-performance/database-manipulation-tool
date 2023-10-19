package io.debezium.performance.dmt.async;

import io.debezium.performance.dmt.dao.Dao;
import io.debezium.performance.dmt.dao.DaoManager;
import io.quarkus.runtime.Startup;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@ApplicationScoped
@Startup
public class ExecutorPool {
    private final ExecutorService pool;
    private final BlockingQueue<RunnableUpsert> runnableUpsertsQueue;
    private final BlockingQueue<RunnableBatchUpsert> runnableBatchUpsertsQueue;
    private final BlockingQueue<RunnableUpsertSecond> runnableUpsertSecondQueue;
    private CountDownLatch latch;


    @Inject
    public ExecutorPool(@ConfigProperty(name = "executor.size", defaultValue = "10") int poolSize, DaoManager manager) {
        pool = Executors.newFixedThreadPool(poolSize);
        latch = new CountDownLatch(0);
        runnableUpsertsQueue = new ArrayBlockingQueue<>(poolSize);
        runnableBatchUpsertsQueue = new ArrayBlockingQueue<>(poolSize);
        runnableUpsertSecondQueue = new ArrayBlockingQueue<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            // For java insertion/update method change the Runnable.
            runnableUpsertsQueue.add(new RunnablePreparedUpsert(manager.getEnabledDbs()));
            runnableBatchUpsertsQueue.add(new RunnableBatchUpsert(manager.getEnabledDbs()));
            runnableUpsertSecondQueue.add(new RunnableUpsertSecond(manager.getEnabledDbs()));
        }
    }

    public void execute(String sqlQuery) {
        RunnableUpsert task;
        try {
            task = runnableUpsertsQueue.take();
        } catch (InterruptedException e) {
            return;
        }
        pool.submit(() -> {
            task.setSqlQuery(sqlQuery);
            task.run();
            try {
                runnableUpsertsQueue.put(task);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
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

    public void executeQuery(String sqlQuery) {
        RunnableUpsertSecond task;
        try {
            task = runnableUpsertSecondQueue.take();
        } catch (InterruptedException e) {
            return;
        }
        pool.submit(() -> {
           task.setDaoFunctionAndExecute(dao -> dao.executeStatement(sqlQuery));
            try {
                runnableUpsertSecondQueue.put(task);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
        });
    }

    public void executeBatchQuery(List<String> sqlQueries) {
        RunnableUpsertSecond task;
        try {
            task = runnableUpsertSecondQueue.take();
        } catch (InterruptedException e) {
            return;
        }
        pool.submit(() -> {
            task.setDaoFunctionAndExecute(dao -> dao.executeBatchStatement(sqlQueries));
            try {
                runnableUpsertSecondQueue.put(task);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
        });
    }

    public void executeFunction(Consumer<Dao> daoFunction) {
        RunnableUpsertSecond task;
        try {
            task = runnableUpsertSecondQueue.take();
        } catch (InterruptedException e) {
            return;
        }
        pool.submit(() -> {
            task.setDaoFunctionAndExecute(daoFunction);
            try {
                runnableUpsertSecondQueue.put(task);
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
