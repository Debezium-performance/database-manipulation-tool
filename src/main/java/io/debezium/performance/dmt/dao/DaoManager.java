/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.CDI;
import javax.inject.Inject;
import javax.inject.Singleton;


@Singleton
public final class DaoManager {
    List<Dao> enabledDbs;

    @Inject
    Instance<MysqlDao> mysql;
    @Inject
    Instance<PostgresDao> postgres;
    @Inject
    Instance<MongoDao> mongo;

    public DaoManager() {
        enabledDbs = new ArrayList<>();
        if (CDI.current().select(PostgresDao.class).isResolvable()) {
            enabledDbs.add(CDI.current().select(PostgresDao.class).get());
        }
        if (CDI.current().select(MysqlDao.class).isResolvable()) {
            enabledDbs.add(CDI.current().select(MysqlDao.class).get());
        }
        if (CDI.current().select(MongoDao.class).isResolvable()) {
            enabledDbs.add(CDI.current().select(MongoDao.class).get());
        }
    }

    public List<Dao> getEnabledDbs() {
        List<Dao> returnList = new ArrayList<>();
        if (postgres.isResolvable()) {
            returnList.add(postgres.get());
        }
        if (mongo.isResolvable()) {
            returnList.add(mongo.get());
        }
        if (mysql.isResolvable()) {
            returnList.add(mysql.get());
        }
        return returnList;
    }

    public List<String> getEnabledDbsNames() {
        return enabledDbs.stream().map(this::prettifyDaoName)
                .collect(Collectors.toList());
    }

    public String prettifyDaoName(Dao dao) {
        String daoName = dao.getClass().getSimpleName();
        String split = daoName.split("_")[0];
        return split.substring(0, split.length() - 3);
    }

    public MongoDao getMongoDao() {
        if (mongo.isResolvable()) {
            return mongo.get();
        }
        return null;
    }
}
