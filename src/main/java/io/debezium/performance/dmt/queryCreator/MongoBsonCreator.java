/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.queryCreator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;

import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import io.debezium.performance.dmt.model.DatabaseColumnEntry;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;

import com.mongodb.client.model.Updates;

import io.debezium.performance.dmt.model.DatabaseEntry;

@ApplicationScoped
public class MongoBsonCreator {
    private static final Logger LOG = Logger.getLogger(MongoBsonCreator.class);

    public Bson updateBson(DatabaseEntry databaseEntry) {
        return Updates.combine(
                databaseEntry.getColumnEntries().stream()
                        .map(columnEntry -> Updates.set(columnEntry.columnName(), columnEntry.value()))
                        .collect(Collectors.toList()));
    }

    public Document insertDocument(DatabaseEntry databaseEntry) {
        Document document = new Document();
        databaseEntry.getColumnEntries()
                .forEach(columnEntry -> document.put(columnEntry.columnName(), columnEntry.value()));
        return document;
    }

    public List<WriteModel<Document>> bulkUpdateBson(List<DatabaseEntry> databaseEntries) {
        List<WriteModel<Document>> bulkOperations = new ArrayList<>();
        for (DatabaseEntry databaseEntry: databaseEntries) {
            Bson filter = getPrimaryFilter(databaseEntry);
            Bson update = updateBson(databaseEntry);
            UpdateOptions options = new UpdateOptions().upsert(true);
            UpdateOneModel<Document> updateOneModel = new UpdateOneModel<>(filter, update, options);
            bulkOperations.add(updateOneModel);
        }
        return bulkOperations;
    }

    public Bson getPrimaryFilter(DatabaseEntry databaseEntry) {
        DatabaseColumnEntry primary = databaseEntry.getPrimaryColumnEntry();
        return Filters.eq(primary.columnName(), primary.value());
    }
}
