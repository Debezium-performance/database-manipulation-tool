package io.debezium.performance.dmt.async;

import io.debezium.performance.dmt.dao.Dao;

import java.util.ArrayList;
import java.util.List;

public class RunnableBatchUpsert extends RunnableUpsert {
    List<String> sqlQueriesBatch;
    public RunnableBatchUpsert(List<Dao> dbs) {
        super(dbs);
        sqlQueriesBatch = new ArrayList<>();
    }

    @Override
    public void run() {
        executeToDaos(dao -> dao.executeBatchStatement(sqlQueriesBatch));
    }

    public void addQuery(String query) {
        sqlQueriesBatch.add(query);
    }

    public List<String> getSqlQueriesBatch() {
        return sqlQueriesBatch;
    }

    public void setSqlQueriesBatch(List<String> sqlQueriesBatch) {
        this.sqlQueriesBatch = sqlQueriesBatch;
    }
}
