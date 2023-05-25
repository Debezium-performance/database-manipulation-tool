/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.queryCreator;

import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;

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
}
