package io.debezium.utils;


import io.debezium.model.DatabaseColumn;
import io.debezium.model.DatabaseColumnEntry;
import io.debezium.model.DatabaseEntry;
import io.debezium.model.DatabaseTableMetadata;
import io.debezium.service.MainService;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class DatabaseEntryParser implements DataParser<DatabaseEntry> {

    private static final Logger LOG = Logger.getLogger(MainService.class);


    @Override
    public DatabaseEntry parse(JsonObject inputJsonObject) {
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
            LOG.error("Could not parse DatabaseEntity from Json object");
            throw new JsonException("Could not parse DatabaseEntity from Json object", ex.getCause());
        }
        return databaseEntry;
    }
}