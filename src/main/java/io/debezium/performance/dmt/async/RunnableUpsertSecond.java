package io.debezium.performance.dmt.async;

import io.debezium.performance.dmt.dao.Dao;
import io.debezium.performance.dmt.dao.MongoDao;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class RunnableUpsertSecond implements Runnable {
    List<Dao> dbs;
    MongoDao mongoDao;
    Consumer<Dao> daoFunction;

    public RunnableUpsertSecond(List<Dao> dbs) {
        this.dbs = new ArrayList<>();
        for (Dao dao: dbs) {
            if (dao.getClass().getName().contains("Mongo")) {
                this.mongoDao = (MongoDao) dao;
            } else {
                this.dbs.add(dao);
            }
        }
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

    public void executeMongoFunction(Consumer<MongoDao> daoFunction) {
        daoFunction.accept(mongoDao);
    }
}
