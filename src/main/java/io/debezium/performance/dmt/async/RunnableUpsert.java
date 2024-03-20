package io.debezium.performance.dmt.async;

import io.debezium.performance.dmt.dao.Dao;

import java.util.List;
import java.util.function.Consumer;

public class RunnableUpsert implements Runnable {

    List<Dao> dbs;
    String sqlQuery;

    public RunnableUpsert(List<Dao> dbs) {
        this.dbs = dbs;
    }

    @Override
    public void run() {
        executeToDaos(dao -> dao.executeStatement(sqlQuery));
    }

    protected void executeToDaos(Consumer<Dao> func) {
        for (Dao dao : dbs) {
            func.accept(dao);
        }
    }

    public String getSqlQuery() {
        return sqlQuery;
    }

    public void setSqlQuery(String sqlQuery) {
        this.sqlQuery = sqlQuery;
    }
}
