/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.logging.Logger;

import io.debezium.performance.dmt.exception.InnerDatabaseException;

public class DatabaseTable {
    private Map<String, DatabaseEntry> rows;
    private DatabaseTableMetadata metadata;

    private static final Logger LOG = Logger.getLogger(DatabaseTable.class);

    public DatabaseTable() {
        rows = new HashMap<>();
        metadata = new DatabaseTableMetadata();
    }

    public DatabaseTable(DatabaseTableMetadata metadata) {
        rows = new HashMap<>();
        this.metadata = metadata;
    }

    public boolean putRow(DatabaseEntry row) {
        boolean updated = rowExists(row);
        String primary = getPrimary(row);
        rows.put(primary, row);
        return updated;
    }

    public void deleteRow(DatabaseEntry row) {
        String primary = getPrimary(row);
        if (rows.remove(primary) == null) {
            LOG.debug("Row did not exist -> not deleting entry " + row);
        }
        else {
            LOG.debug("Row successfully deleted " + row);
        }
    }

    public boolean rowExists(DatabaseEntry row) {
        return rows.containsKey(getPrimary(row));
    }

    private String getPrimary(DatabaseEntry row) {
        DatabaseColumnEntry primary = row.getPrimaryColumnEntry();
        if (primary != null) {
            return primary.value();
        }
        throw new InnerDatabaseException("Primary key is not set");
    }

    public DatabaseTableMetadata getMetadata() {
        return metadata;
    }

    public List<DatabaseEntry> getRows() {
        return new ArrayList<>(rows.values());
    }

    public Map<String, DatabaseEntry> getRowsAsMap() {
        return rows;
    }
}
