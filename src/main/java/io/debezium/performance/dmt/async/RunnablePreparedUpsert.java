package io.debezium.performance.dmt.async;

import io.debezium.performance.dmt.dao.Dao;

import java.util.List;

public class RunnablePreparedUpsert extends RunnableUpsert {

    public RunnablePreparedUpsert(List<Dao> dbs) {
        super(dbs);
    }

    @Override
    public void run() {
        executeToDaos(dao -> dao.executePreparedStatement(sqlQuery));
    }
}
