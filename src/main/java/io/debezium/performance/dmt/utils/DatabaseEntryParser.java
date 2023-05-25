/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.utils;

import java.util.ArrayList;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.jboss.logging.Logger;

import io.debezium.performance.dmt.schema.DatabaseColumn;
import io.debezium.performance.dmt.schema.DatabaseColumnEntry;
import io.debezium.performance.dmt.schema.DatabaseEntry;
import io.debezium.performance.dmt.schema.DatabaseTableMetadata;
import io.debezium.performance.dmt.service.MainService;

@ApplicationScoped
public class DatabaseEntryParser implements DataParser<DatabaseEntry> {

    private static final Logger LOG = Logger.getLogger(MainService.class);

    @Override
    public DatabaseEntry parse(JsonObject inputJsonObject) throws JsonException {
        DatabaseEntry databaseEntry;
        try {
            List<DatabaseColumnEntry> entries = new ArrayList<>();
            DatabaseTableMetadata table = new DatabaseTableMetadata();

            table.setName(inputJsonObject.getString("table"));
            String primary = inputJsonObject.getString("primary");
            JsonArray payload = inputJsonObject.getJsonArray("payload");

            for (JsonValue rawEntry : payload) {
                JsonObject objectEntry = rawEntry.asJsonObject();
                DatabaseColumnEntry entry = new DatabaseColumnEntry(objectEntry.getString("value"), objectEntry.getString("name"), objectEntry.getString("dataType"));
                entries.add(entry);
                table.addColumn(new DatabaseColumn(entry.columnName(), entry.dataType(), primary.equals(entry.columnName())));
            }
            databaseEntry = new DatabaseEntry(entries, table);
        }
        catch (Exception ex) {
            LOG.error("Could not parse DatabaseEntity from Json object " + inputJsonObject);
            throw new JsonException("Could not parse DatabaseEntity from Json object", ex.getCause());
        }
        return databaseEntry;
    }
}
