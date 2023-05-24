/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.enterprise.context.RequestScoped;

import io.debezium.DatabaseColumn;
import io.debezium.DatabaseEntry;
import io.debezium.DatabaseTableMetadata;
import org.jboss.logging.Logger;

import io.debezium.dataSource.DataSourceWrapper;
import io.debezium.exception.RuntimeSQLException;
import io.debezium.queryCreator.QueryCreator;

@SuppressWarnings("CdiManagedBeanInconsistencyInspection")
@RequestScoped
public abstract class AbstractBasicDao implements Dao {

    protected DataSourceWrapper source;
    protected QueryCreator queryCreator;

    protected final Logger LOG = Logger.getLogger(getClass());

    public AbstractBasicDao() {
    }

    public AbstractBasicDao(DataSourceWrapper source, QueryCreator queryCreator) {
        this.source = source;
        this.queryCreator = queryCreator;
    }

    @Override
    public void insert(DatabaseEntry databaseEntry) {
        try (Connection conn = source.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(queryCreator.insertQuery(databaseEntry));
        }
        catch (SQLException ex) {
            LOG.error("Could not insert into database " + databaseEntry);
            LOG.error(ex.getMessage());
            throw new RuntimeSQLException(ex);
        }
    }

    @Override
    public void update(DatabaseEntry databaseEntry) {
        try (Connection conn = source.getConnection();
                Statement stmt = conn.createStatement()) {
            if (databaseEntry.getPrimaryColumnEntry() == null) {
                throw new RuntimeException("Cannot update without primary key");
            }
            stmt.execute(queryCreator.updateQuery(databaseEntry));
        }
        catch (Exception ex) {
            LOG.error("Could not update database " + databaseEntry);
            LOG.error(ex.getMessage());
            throw new RuntimeSQLException(ex);
        }
    }

    @Override
    public void createTable(DatabaseEntry databaseEntry) {
        DatabaseTableMetadata metadata = databaseEntry.getDatabaseTableMetadata();
        try (Connection conn = source.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(queryCreator.createTableQuery(metadata));
        }
        catch (SQLException ex) {
            LOG.error("Could not create table " + metadata);
            LOG.error(ex.getMessage());
            throw new RuntimeSQLException(ex);
        }
    }

    @Override
    public void alterTable(List<DatabaseColumn> columns, DatabaseTableMetadata metadata) {
        try (Connection conn = source.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(queryCreator.addColumnsQuery(columns, metadata.getName()));
        }
        catch (SQLException ex) {
            LOG.error("Could not add columns " + columns + " to table " + metadata.getName());
            LOG.error(ex.getMessage());
            throw new RuntimeSQLException(ex);
        }
    }

    @Override
    public void dropTable(DatabaseEntry databaseEntry) {
        DatabaseTableMetadata metadata = databaseEntry.getDatabaseTableMetadata();
        try (Connection conn = source.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(queryCreator.dropTable(metadata));
        }
        catch (SQLException ex) {
            LOG.error("Could not drop table " + metadata.getName());
            LOG.error(ex.getMessage());
            throw new RuntimeSQLException(ex);
        }
    }

    @Override
    public void delete(DatabaseEntry databaseEntry) {

    }

    public DataSourceWrapper getSource() {
        return source;
    }

    public QueryCreator getQueryCreator() {
        return queryCreator;
    }
}
