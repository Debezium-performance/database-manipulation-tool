package io.debezium.model;

import io.debezium.exception.InnerDatabaseException;
import org.jboss.logging.Logger;

import java.util.*;

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

    public boolean putRow (DatabaseEntry row) {
        boolean updated = rowExists(row);
        String primary = getPrimary(row);
        rows.put(primary, row);
        return updated;
    }

    public void deleteRow (DatabaseEntry row) {
        String primary = getPrimary(row);
        if (rows.remove(primary) == null) {
            LOG.debug("Row did not exist -> not deleting entry " + row);
        } else {
            LOG.debug("Row successfully deleted " + row);
        }
    }

    public boolean rowExists (DatabaseEntry row) {
        return rows.containsKey(getPrimary(row));
    }

    private String getPrimary (DatabaseEntry row) {
        Optional<DatabaseColumnEntry> primary = row.getPrimaryColumnEntry();
        if (primary.isPresent()) {
            return primary.get().value();
        }
        throw new InnerDatabaseException("Primary key is not set");
    }

    public DatabaseTableMetadata getMetadata() {
        return metadata;
    }

    public List<DatabaseEntry> getRows() {
        return new ArrayList<>(rows.values());
    }
}
