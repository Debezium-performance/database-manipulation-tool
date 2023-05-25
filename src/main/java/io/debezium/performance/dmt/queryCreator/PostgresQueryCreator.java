/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.queryCreator;

import javax.enterprise.context.ApplicationScoped;

import org.jboss.logging.Logger;

import io.debezium.performance.dmt.schema.DatabaseColumn;
import io.debezium.performance.dmt.schema.DatabaseTableMetadata;

@ApplicationScoped
public class PostgresQueryCreator extends AbstractBasicQueryCreator {

    private static final Logger LOG = Logger.getLogger(PostgresQueryCreator.class);

    public PostgresQueryCreator() {
    }

    @Override
    public String createTableQuery(DatabaseTableMetadata databaseTableMetadata) {
        StringBuilder builder = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(databaseTableMetadata.getName())
                .append(" (");

        for (DatabaseColumn column : databaseTableMetadata.getColumns()) {
            builder.append(column.getName())
                    .append(" ")
                    .append(convertDouble(column.getDataType()))
                    .append(", ");
        }
        builder.append("PRIMARY KEY (")
                .append(databaseTableMetadata.getPrimary().getName())
                .append("), ");

        builder.delete(builder.length() - 2, builder.length())
                .append(")");
        String query = builder.toString();
        LOG.debug("CREATED TABLE CREATE QUERY: " + query);
        return query;
    }

    @Override
    public String dropDatabase(String schema) {
        String query = "DROP SCHEMA IF EXISTS " + schema + " CASCADE";
        LOG.debug("Created DROP SCHEMA query: " + query);
        return query;
    }

    @Override
    public String createDatabase(String schema) {
        String query = "CREATE SCHEMA " + schema;
        LOG.debug("Created CREATE SCHEMA query: " + query);
        return query;
    }

    private String convertDouble(String dataType) {
        if (dataType.equalsIgnoreCase("double")) {
            return "Double Precision";
        }
        return dataType;
    }
}
