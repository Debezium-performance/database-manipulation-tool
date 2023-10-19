package io.debezium.performance.dmt.async;

import io.debezium.performance.dmt.dao.Dao;

import java.util.List;
import java.util.function.Consumer;

public class RunnableUpsertSecond implements Runnable {
    List<Dao> dbs;
    Consumer<Dao> daoFunction;

    public RunnableUpsertSecond(List<Dao> dbs) {
        this.dbs = dbs;
    }

    @Override
    public void run()
    {
        for (Dao dao : dbs) {
            daoFunction.accept(dao);
        }
    }

    public Consumer<Dao> getDaoFunction() {
        return daoFunction;
    }

    public void setDaoFunction(Consumer<Dao> daoFunction) {
        this.daoFunction = daoFunction;
    }

    public void setDaoFunctionAndExecute(Consumer<Dao> daoFunction) {
        setDaoFunction(daoFunction);
        run();
    }
}
