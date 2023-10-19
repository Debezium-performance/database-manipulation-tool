package io.debezium.performance.dmt.service;

import com.mongodb.client.model.WriteModel;
import io.debezium.performance.dmt.async.ExecutorPool;
import io.debezium.performance.dmt.dao.MongoDao;
import io.debezium.performance.dmt.generator.Generator;
import io.debezium.performance.dmt.model.DatabaseEntry;
import io.debezium.performance.dmt.queryCreator.MongoBsonCreator;
import io.debezium.performance.dmt.queryCreator.MysqlQueryCreator;
import io.quarkus.runtime.Startup;
import org.bson.Document;

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
    @Inject
    MongoBsonCreator mongoBsonCreator;

    public long[] createAndExecuteLoad(int count, int maxRows) {
        List<String> statements = generateAviationSqlQueries(count, maxRows);
        executorPool.setCountDownLatch(statements.size());
        long start = System.currentTimeMillis();
        for (String statement: statements) {
            executorPool.execute(statement);
        }
        return waitForLastTask(start);
    }

    public long[] createAndExecuteBatchLoad(int count, int maxRows) {
        List<String> queries = generateAviationSqlQueries(count, maxRows);
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

    public long[] createAndExecuteBatchLoadSecond(int count, int maxRows) {
        List<String> queries = generateAviationSqlQueries(count, maxRows);
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
            executorPool.executeFunction(dao -> dao.executeBatchStatement(batch));
        }
        return waitForLastTask(start);
    }

    public long createAndExecuteSizedMongoLoad(int count, int maxRows, int messageSize) {
        String collection = generator.generateByteBatch(1,1,1).get(0).getDatabaseTableMetadata().getName();
        List<WriteModel<Document>> bulkOperations = generateSizedMongoBulk(count, maxRows, messageSize);
        MongoDao mongo = daoManager.getMongoDao();
        long start = System.currentTimeMillis();
        mongo.bulkWrite(bulkOperations, collection);
        long end = System.currentTimeMillis();
        return end - start;
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

    private List<String> generateAviationSqlQueries(int count, int maxRows) {
        List<DatabaseEntry> entries = generator.generateAviationBatch(count, maxRows);
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

    private List<WriteModel<Document>> generateSizedMongoBulk(int count, int maxRows, int messageSize) {
        List<DatabaseEntry> entries = generator.generateByteBatch(count, maxRows, messageSize);
        createTable(entries.get(0));
        for (DatabaseEntry entry: entries) {
            database.upsertEntry(entry);
        }
        return mongoBsonCreator.bulkUpdateBson(entries);
    }

}
