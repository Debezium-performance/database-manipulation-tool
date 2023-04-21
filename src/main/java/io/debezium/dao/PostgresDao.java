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
import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.jboss.logging.Logger;

import io.agroal.api.AgroalDataSource;
import io.debezium.entity.DatabaseColumnEntry;
import io.debezium.entity.DatabaseEntry;
import io.debezium.queryCreator.PostgresQueryCreator;
import io.quarkus.agroal.DataSource;
import io.quarkus.arc.Unremovable;
import io.quarkus.arc.lookup.LookupIfProperty;

@ApplicationScoped
@LookupIfProperty(name = "quarkus.datasource.postgresql.enabled", stringValue = "true")
@Unremovable
public class PostgresDao implements Dao {
    @Inject
    @DataSource("postgresql")
    AgroalDataSource source;

    @Inject
    PostgresQueryCreator queryCreator;
    private static final Logger LOG = Logger.getLogger(PostgresDao.class);
    private static final String DB_NAME = "Postgresql";

    @Override
    public Optional<DatabaseEntry> get(long id) {
        return Optional.empty();
    }

    @Override
    public List<DatabaseEntry> getAll() {
        return null;
    }

    @Override
    public void insert(DatabaseEntry databaseEntry) {
        try (Connection conn = source.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(queryCreator.insertQuery(databaseEntry));
        }
        catch (SQLException ex) {
            LOG.error("Could not insert into database " + databaseEntry);
        }
    }

    @Override
    public void delete(DatabaseEntry databaseEntry) {

    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public void update(DatabaseEntry databaseEntry) {
        try (Connection conn = source.getConnection();
                Statement stmt = conn.createStatement()) {
            if (databaseEntry.getPrimaryColumnEntry().isEmpty()) {
                throw new RuntimeException("Cannot update without primary key");
            }
            stmt.execute(queryCreator.updateQuery(databaseEntry));
        }
        catch (Exception ex) {
            LOG.error("Could not update database " + databaseEntry);
            LOG.error(ex);
        }
    }

    @Override
    public void upsert(DatabaseEntry databaseEntry) {
        try (Connection conn = source.getConnection();
                Statement stmt = conn.createStatement()) {
            Optional<DatabaseColumnEntry> primary = databaseEntry.getPrimaryColumnEntry();
            if (primary.isEmpty()) {
                insert(databaseEntry);
                return;
            }
            stmt.execute(queryCreator.upsertQuery(databaseEntry));
            LOG.debug("Successful upsert " + databaseEntry);
        }
        catch (SQLException ex) {
            LOG.error("Could not upsert " + databaseEntry);
            LOG.error(ex);
        }
    }

    @Override
    public void createTable(DatabaseEntry databaseEntry) {
        try (Connection conn = source.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(queryCreator.createTableQuery(databaseEntry.getDatabaseTable()));
        }
        catch (SQLException ex) {
            LOG.error("Could not create table " + databaseEntry);
            LOG.error(ex);
        }
    }

    @Override
    public void createTableAndInsert(DatabaseEntry databaseEntry) {

    }

    @Override
    public void createTableAndUpsert(DatabaseEntry databaseEntry) {
        createTable(databaseEntry);
        upsert(databaseEntry);
    }
}
