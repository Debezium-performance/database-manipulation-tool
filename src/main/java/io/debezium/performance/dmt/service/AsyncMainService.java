package io.debezium.performance.dmt.service;

import io.debezium.performance.dmt.async.ExecutorPool;
import io.debezium.performance.dmt.generator.Generator;
import io.debezium.performance.dmt.model.DatabaseEntry;
import io.debezium.performance.dmt.queryCreator.MysqlQueryCreator;
import io.quarkus.runtime.Startup;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@RequestScoped
@Startup
public class AsyncMainService extends MainService {
    @Inject
    Generator generator;
    @Inject
    ExecutorPool executorPool;
    @Inject
    MysqlQueryCreator mysqlQueryCreator;
    public long[] generateLoad(int count, int maxRows) {
        List<String> statements = generateQueries(count, maxRows);
        executorPool.setCountDownLatch(statements.size());
        long start = System.currentTimeMillis();
        for (String statement: statements) {
            executorPool.execute(statement);
        }
        return waitForLastTask(start);
    }

    public long[] generateBatchLoad(int count, int maxRows) {
        List<String> queries = generateQueries(count,maxRows);
        int poolSize = executorPool.getPoolSize();
        int batchSize = queries.size() / poolSize;
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < queries.size(); i += batchSize) {
            batches.add(queries.subList(i, Math.min(i + batchSize, queries.size())));
        }
        executorPool.setCountDownLatch(batches.size());
        long start = System.currentTimeMillis();
        for (List<String> batch: batches) {
            executorPool.executeBatch(batch);
        }
        return waitForLastTask(start);
    }

    private long[] waitForLastTask(long start) {
        long lastThreadExecuted = System.currentTimeMillis() - start;
        long lastThreadFinished;
        try {
            executorPool.getLatch().await();
            lastThreadFinished = System.currentTimeMillis() - start;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return new long[] {lastThreadExecuted, lastThreadFinished};
    }

    private List<String> generateQueries(int count, int maxRows) {
        List<DatabaseEntry> entries = generator.generateBatch(count, maxRows);
        createTable(entries.get(0));
        List<String> queries = new ArrayList<>();
        for (DatabaseEntry entry: entries) {
            if (database.upsertEntry(entry)) {
                queries.add(mysqlQueryCreator.updateQuery(entry));
            } else {
                queries.add(mysqlQueryCreator.insertQuery(entry));
            }
        }
        return queries;
    }

}
