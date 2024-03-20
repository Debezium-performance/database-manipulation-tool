package io.debezium.performance.dmt.parser;

import io.debezium.performance.dmt.model.DatabaseColumn;
import io.debezium.performance.dmt.model.DatabaseEntry;
import io.debezium.performance.dmt.model.DatabaseTableMetadata;
import io.debezium.performance.dmt.model.DatabaseColumnEntry;

import java.util.ArrayList;
import java.util.List;

public class ParseUtils {
    public static DatabaseEntry fromSchema (io.debezium.performance.dmt.schema.DatabaseEntry inputObject) {
        DatabaseEntry databaseEntry;
        List<DatabaseColumnEntry> entries = new ArrayList<>();
        DatabaseTableMetadata table = new DatabaseTableMetadata();

        table.setName(inputObject.getName());
        String primary = inputObject.getPrimary();

        for (var databaseColumnEntry : inputObject.getColumnEntries()) {
            DatabaseColumnEntry entry = new DatabaseColumnEntry(databaseColumnEntry.value(), databaseColumnEntry.columnName(), databaseColumnEntry.dataType());
            entries.add(entry);
            table.addColumn(new DatabaseColumn(entry.columnName(), entry.dataType(), primary.equals(entry.columnName())));
        }
        databaseEntry = new DatabaseEntry(entries, table);
        return databaseEntry;
    }
}
