/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;

import io.debezium.DatabaseColumn;
import io.debezium.DatabaseEntry;
import io.debezium.DatabaseTableMetadata;
import org.jboss.logging.Logger;

import io.debezium.exception.InnerDatabaseException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.prometheus.PrometheusMeterRegistry;

@ApplicationScoped
public class Database {
    private final MeterRegistry registry;
    Map<String, DatabaseTable> tables;

    private static final Logger LOG = Logger.getLogger(Database.class);

    public Database(PrometheusMeterRegistry registry) {
        tables = new HashMap<>();
        this.registry = registry;
        registry.gaugeMapSize("table.count", Tags.empty(), tables);
    }

    public boolean tableExists(String name) {
        return tables.containsKey(name);
    }

    public DatabaseTable getTable(String name) {
        return tables.get(name);
    }

    public boolean entryExists(DatabaseEntry entry) {
        String tableName = entry.getDatabaseTableMetadata().getName();
        return tableExists(tableName) && getTable(tableName).rowExists(entry);
    }

    /**
     * @param tableMetadata contains name that is checked
     * @return false if table exists, true if it was created.
     */
    public boolean createTableIfNotExists(DatabaseTableMetadata tableMetadata) {
        if (tableExists(tableMetadata.getName())) {
            return false;
        }
        LOG.debug("Creating table in inner database: " + tableMetadata.getName());
        createTable(tableMetadata);
        return true;
    }

    /**
     * @param tableMetadata contains columns that are required in the database table
     * @return List of columns added. Is empty table did not need alteration. Null the table was created.
     */
    public List<DatabaseColumn> createOrAlterTable(DatabaseTableMetadata tableMetadata) {
        if (createTableIfNotExists(tableMetadata)) {
            return null;
        }
        DatabaseTableMetadata current = tables.get(tableMetadata.getName()).getMetadata();
        List<DatabaseColumn> missingColumns = tableMetadata.getMissingColumns(current);
        missingColumns.forEach(current::addColumn);
        return missingColumns;
    }

    public void dropTable(String name) {
        tables.remove(name);
    }

    public void resetDatabase() {
        tables.clear();
    }

    /**
     * @param entry
     * @return true if updated, false if inserted
     */
    public boolean upsertEntry(DatabaseEntry entry) {
        String tableName = entry.getDatabaseTableMetadata().getName();
        if (!entry.getDatabaseTableMetadata().isSubsetOf(getTable(tableName).getMetadata())) {
            throw new InnerDatabaseException("Entry contains columns the database table does not");
        }
        return getTable(tableName).putRow(entry);
    }

    public void insertEntry(DatabaseEntry entry) {
        String tableName = entry.getDatabaseTableMetadata().getName();
        createTableIfNotExists(entry.getDatabaseTableMetadata());
        if (entryExists(entry)) {
            throw new InnerDatabaseException("Table already contains entry");
        }
        if (!entry.getDatabaseTableMetadata().isSubsetOf(getTable(tableName).getMetadata())) {
            throw new InnerDatabaseException("Entry contains columns the database does not");
        }
        getTable(tableName).putRow(entry);
    }

    public void deleteEntry(DatabaseEntry entry) {
        DatabaseTable table = tables.get(entry.getDatabaseTableMetadata().getName());
        if (table == null) {
            LOG.debug("Table does not exist -> not deleting entry " + entry);
            return;
        }
        table.deleteRow(entry);
    }

    public List<DatabaseTable> getTables() {
        return new ArrayList<>(tables.values());
    }

    private void createTable(DatabaseTableMetadata tableMetadata) {
        tables.put(tableMetadata.getName(), new DatabaseTable(tableMetadata));
        registry.gaugeMapSize(tableMetadata.getName() + ".table.size", Tags.empty(), tables.get(tableMetadata.getName()).getRowsAsMap());
    }
}
