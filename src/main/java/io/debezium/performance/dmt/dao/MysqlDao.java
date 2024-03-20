/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.enterprise.context.Dependent;
import javax.inject.Inject;


import io.debezium.performance.dmt.dataSource.MysqlDataSource;
import io.debezium.performance.dmt.exception.RuntimeSQLException;
import io.debezium.performance.dmt.queryCreator.MysqlQueryCreator;
import io.quarkus.arc.Unremovable;
import io.quarkus.arc.lookup.LookupIfProperty;

@Dependent
@LookupIfProperty(name = "quarkus.datasource.mysql.enabled", stringValue = "true")
@Unremovable
public final class MysqlDao extends AbstractBasicDao {

    @Inject
    public MysqlDao(MysqlDataSource source, MysqlQueryCreator queryCreator) {
        super(source, queryCreator);
    }

    @Override
    public void resetDatabase() {
        try (Connection conn = source.getConnection();
                Statement stmt = conn.createStatement()) {
            String schema = conn.getCatalog();
            stmt.execute(queryCreator.dropDatabase(schema));
            stmt.execute(queryCreator.createDatabase(schema));
            stmt.execute(((MysqlQueryCreator) queryCreator).useDatabase(schema));
            LOG.info("Successfully reset database");
        }
        catch (SQLException ex) {
            LOG.error("Could not reset database");
            LOG.error(ex.getMessage());
            throw new RuntimeSQLException(ex);
        }
    }
}
