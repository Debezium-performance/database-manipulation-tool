/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.queryCreator;

import java.util.List;

import org.jboss.logging.Logger;

import io.debezium.performance.dmt.model.DatabaseColumn;
import io.debezium.performance.dmt.model.DatabaseColumnEntry;
import io.debezium.performance.dmt.model.DatabaseEntry;
import io.debezium.performance.dmt.model.DatabaseTableMetadata;

public abstract class AbstractBasicQueryCreator implements QueryCreator {

    protected final Logger LOG = Logger.getLogger(getClass());

    @Override
    public String insertQuery(DatabaseEntry databaseEntry) {
        StringBuilder builder = new StringBuilder("INSERT INTO ")
                .append(databaseEntry.getDatabaseTableMetadata().getName())
                .append(" (");

        for (DatabaseColumn column : databaseEntry.getColumns()) {
            builder.append(column.getName())
                    .append(", ");
        }
        builder.delete(builder.length() - 2, builder.length())
                .append(") VALUES (");

        for (DatabaseColumnEntry entry : databaseEntry.getColumnEntries()) {
            builder.append('\'')
                    .append(entry.value())
                    .append('\'')
                    .append(", ");
        }
        builder.delete(builder.length() - 2, builder.length())
                .append(")");
        return builder.toString();
    }

    @Override
    public String createTableQuery(DatabaseTableMetadata databaseTableMetadata) {
        StringBuilder builder = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(databaseTableMetadata.getName())
                .append(" (");

        for (DatabaseColumn column : databaseTableMetadata.getColumns()) {
            builder.append(column.getName())
                    .append(" ")
                    .append(column.getDataType())
                    .append(", ");
        }
        builder.append("PRIMARY KEY (")
                .append(databaseTableMetadata.getPrimary().getName())
                .append("), ");
        builder.delete(builder.length() - 2, builder.length())
                .append(")");
        String query = builder.toString();
        LOG.debug("Created TABLE CREATE query: " + query);
        return query;
    }

    /**
     *
     * @param databaseEntry with primary attribute
     * @return Update query for databaseEntry
     */
    @Override
    public String updateQuery(DatabaseEntry databaseEntry) {
        StringBuilder builder = new StringBuilder("UPDATE ")
                .append(databaseEntry.getDatabaseTableMetadata().getName())
                .append(" SET ");
        for (DatabaseColumnEntry columnEntry : databaseEntry.getColumnEntries()) {
            builder.append(columnEntry.columnName())
                    .append(" = '")
                    .append(columnEntry.value())
                    .append("', ");
        }
        builder.delete(builder.length() - 2, builder.length())
                .append(" WHERE ")
                .append(databaseEntry.getPrimaryColumnEntry().columnName())
                .append(" = '")
                .append(databaseEntry.getPrimaryColumnEntry().value())
                .append("'");

        return builder.toString();
    }

    @Override
    public String addColumnsQuery(List<DatabaseColumn> columns, String databaseName) {
        StringBuilder builder = new StringBuilder("ALTER TABLE " + databaseName);
        for (DatabaseColumn column : columns) {
            builder.append(" ADD COLUMN ")
                    .append(column.getName())
                    .append(" ")
                    .append(column.getDataType())
                    .append(", ");
        }
        builder.delete(builder.length() - 2, builder.length());
        String query = builder.toString();
        LOG.debug("Created ALTER TABLE query: " + query);
        return query;
    }

    @Override
    public String dropTable(DatabaseTableMetadata databaseTableMetadata) {
        String query = "DROP TABLE IF EXISTS " + databaseTableMetadata.getName();
        LOG.debug("Created DROP TABLE query: " + query);
        return query;
    }
}
