/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.utils;

import java.util.List;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import io.debezium.DatabaseColumn;
import io.debezium.DatabaseColumnEntry;
import io.debezium.DatabaseEntry;
import io.debezium.model.Database;
import io.debezium.model.DatabaseTable;

public class ResponseJsonBuilder {
    JsonObjectBuilder mainBuilder;
    JsonArrayBuilder tablesBuilder;

    public ResponseJsonBuilder() {
        this.mainBuilder = Json.createObjectBuilder();
        this.tablesBuilder = Json.createArrayBuilder();
    }

    public ResponseJsonBuilder(Database database) {
        this.mainBuilder = Json.createObjectBuilder();
        this.tablesBuilder = Json.createArrayBuilder();
        database.getTables().forEach(this::addTable);
    }

    public ResponseJsonBuilder addDatabases(List<String> databases) {
        JsonArrayBuilder databasesBuilder = Json.createArrayBuilder();
        databases.forEach(databasesBuilder::add);
        mainBuilder.add("databases", databasesBuilder);
        return this;
    }

    public ResponseJsonBuilder addTable(DatabaseTable table) {
        JsonObjectBuilder tableBuilder = Json.createObjectBuilder();
        tableBuilder.add("tableName", table.getMetadata().getName());
        tableBuilder.add("primary", table.getMetadata().getPrimary().getName());


        JsonArrayBuilder schemaBuilder = Json.createArrayBuilder();
        for (DatabaseColumn databaseColumn : table.getMetadata().getColumns()) {
            JsonObjectBuilder columnSchemaBuilder = Json.createObjectBuilder();
            columnSchemaBuilder.add("name", databaseColumn.getName());
            columnSchemaBuilder.add("dataType", databaseColumn.getDataType());
            schemaBuilder.add(columnSchemaBuilder);
        }
        tableBuilder.add("tableSchema", schemaBuilder);

        JsonArrayBuilder rowsBuilder = Json.createArrayBuilder();
        int rowNumber = 1;
        for (DatabaseEntry row : table.getRows()) {
            JsonObjectBuilder rowBuilder = Json.createObjectBuilder();
            rowBuilder.add("rowNumber", rowNumber++);
            JsonArrayBuilder columnsBuilder = Json.createArrayBuilder();
            for (DatabaseColumnEntry columnEntry : row.getColumnEntries()) {
                JsonObjectBuilder columnBuilder = Json.createObjectBuilder();
                columnBuilder.add("columnName", columnEntry.columnName());
                columnBuilder.add("columnValue", columnEntry.value());
                columnsBuilder.add(columnBuilder);
            }
            rowBuilder.add("columns", columnsBuilder);
            rowsBuilder.add(rowBuilder);
        }
        tableBuilder.add("tableRows", rowsBuilder);
        tablesBuilder.add(tableBuilder);
        return this;
    }

    public JsonObject build() {
        mainBuilder.add("tables", tablesBuilder);
        return mainBuilder.build();
    }
}
