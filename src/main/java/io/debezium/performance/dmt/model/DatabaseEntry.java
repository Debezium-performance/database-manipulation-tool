/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.model;

import java.util.List;
import java.util.Objects;

public class DatabaseEntry {

    private List<DatabaseColumnEntry> columnEntries;
    private final DatabaseTableMetadata databaseTableMetadata;

    public DatabaseEntry(List<DatabaseColumnEntry> columnEntries, DatabaseTableMetadata databaseTableMetadata) {
        this.columnEntries = columnEntries;
        this.databaseTableMetadata = databaseTableMetadata;
    }

    public DatabaseEntry() {
        databaseTableMetadata = new DatabaseTableMetadata();
    }

    public List<DatabaseColumnEntry> getColumnEntries() {
        return columnEntries;
    }

    public void setColumnEntries(List<DatabaseColumnEntry> columnEntries) {
        this.columnEntries = columnEntries;
    }

    public void addColumnEntry(DatabaseColumnEntry columnEntry) {
        columnEntries.add(columnEntry);
    }

    public List<DatabaseColumn> getColumns() {
        return databaseTableMetadata.getColumns();
    }

    public DatabaseTableMetadata getDatabaseTableMetadata() {
        return databaseTableMetadata;
    }

    /**
     * @return DatabaseColumnEntry which is the primary in this DatabaseEntry
     */
    public DatabaseColumnEntry getPrimaryColumnEntry() {
        DatabaseColumn primaryColumn = databaseTableMetadata.getPrimary();
        for (DatabaseColumnEntry columnEntry : columnEntries) {
            if (columnEntry.columnName().equals(primaryColumn.getName())) {
                return columnEntry;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "DatabaseEntity{" +
                "columnEntries=" + columnEntries +
                ", databaseTable=" + databaseTableMetadata +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatabaseEntry that = (DatabaseEntry) o;
        return columnEntries.equals(that.columnEntries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnEntries);
    }
}
