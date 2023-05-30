/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.service;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.json.JsonObject;

import io.debezium.performance.dmt.utils.TimeJsonBuilder;
import org.jboss.logging.Logger;

import io.debezium.performance.dmt.dao.Dao;
import io.debezium.performance.dmt.dao.DaoManager;
import io.debezium.performance.dmt.exception.InnerDatabaseException;
import io.debezium.performance.dmt.model.Database;
import io.debezium.performance.dmt.model.DatabaseColumn;
import io.debezium.performance.dmt.model.DatabaseEntry;
import io.debezium.performance.dmt.model.DatabaseTableMetadata;

@RequestScoped
public class MainService {
    @Inject
    DaoManager daoManager;

    @Inject
    Database database;

    private static final Logger LOG = Logger.getLogger(MainService.class);

    public void insert(DatabaseEntry databaseEntry) {
        try {
            database.insertEntry(databaseEntry);
        }
        catch (InnerDatabaseException ex) {
            LOG.error("Error when inserting entry into inner database");
            LOG.error(ex.getMessage());
            return; //TODO: Throw Exception out
        }
        executeToDaos(dao -> dao.insert(databaseEntry));
    }

    public void createTable(DatabaseEntry databaseEntry) {
        List<DatabaseColumn> changedColumns;
        try {
            changedColumns = database.createOrAlterTable(databaseEntry.getDatabaseTableMetadata());

        }
        catch (InnerDatabaseException ex) {
            LOG.error("Error when creating table in database");
            LOG.error(ex.getMessage());
            return;
        }
        if (changedColumns == null) {
            LOG.debug("Creating table in Dbs " + databaseEntry.getDatabaseTableMetadata());
            executeToDaos(dao -> dao.createTable(databaseEntry));
            return;
        }

        if (!changedColumns.isEmpty()) {
            LOG.debug("Altering table in Dbs " + databaseEntry.getDatabaseTableMetadata());
            alterTableToDao(changedColumns, databaseEntry.getDatabaseTableMetadata());
        }
    }

    public void upsert(DatabaseEntry databaseEntry) {
        boolean update = false;
        try {
            createTable(databaseEntry);
            update = database.upsertEntry(databaseEntry);
        }
        catch (InnerDatabaseException ex) {
            LOG.error("Error when upserting entry into inner database");
            LOG.error(ex.getMessage());
            throw ex;
        }
        catch (Exception ex) {
            LOG.error("Error when upserting entry into databases");
            LOG.error(ex.getMessage());
        }
        if (update) {
            executeToDaos(dao -> dao.update(databaseEntry));
        }
        else {
            executeToDaos(dao -> dao.insert(databaseEntry));
        }
    }

    public void dropTable(DatabaseEntry databaseEntry) {
        try {
            database.dropTable(databaseEntry.getDatabaseTableMetadata().getName());
        }
        catch (InnerDatabaseException ex) {
            LOG.error("Error when dropping table from inner database");
            LOG.error(ex.getMessage());
            return;
        }
        executeToDaos(dao -> dao.dropTable(databaseEntry));
    }

    public void resetDatabase() {
        database.resetDatabase();
        executeToDaos((Dao::resetDatabase));
    }

    public JsonObject timedInsert(DatabaseEntry databaseEntry) {
        try {
            database.insertEntry(databaseEntry);
        }
        catch (InnerDatabaseException ex) {
            LOG.error("Error when timed inserting entry into inner database");
            LOG.error(ex.getMessage());
            throw ex;
        }
        TimeJsonBuilder builder = new TimeJsonBuilder();
        for (Dao dao: daoManager.getEnabledDbs()) {
            Instant instant = dao.timedInsert(databaseEntry);
            builder.addDbTimestamp(daoManager.prettifyDaoName(dao), instant);
        }
        return builder.build();
    }

    private void alterTableToDao(List<DatabaseColumn> columns, DatabaseTableMetadata metadata) {
        RuntimeException exception = null;
        for (Dao dao : daoManager.getEnabledDbs()) {
            try {
                dao.alterTable(columns, metadata);
            }
            catch (RuntimeException ex) {
                exception = ex;
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    private void executeToDaos(Consumer<Dao> func) {
        RuntimeException exception = null;
        for (Dao dao : daoManager.getEnabledDbs()) {
            try {
                func.accept(dao);
            }
            catch (RuntimeException ex) {
                exception = ex;
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

}
