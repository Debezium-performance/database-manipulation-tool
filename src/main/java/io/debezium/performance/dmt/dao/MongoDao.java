/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.dao;

import java.time.Instant;
import java.util.List;

import javax.enterprise.context.Dependent;
import javax.inject.Inject;

import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import io.debezium.performance.dmt.model.DatabaseColumn;
import io.debezium.performance.dmt.model.DatabaseColumnEntry;
import io.debezium.performance.dmt.model.DatabaseEntry;
import io.debezium.performance.dmt.model.DatabaseTableMetadata;
import io.debezium.performance.dmt.queryCreator.MongoBsonCreator;
import io.quarkus.arc.Unremovable;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.mongodb.MongoClientName;

@Dependent
@LookupIfProperty(name = "quarkus.mongodb.main.enabled", stringValue = "true")
@Unremovable
public final class MongoDao implements Dao {

    @ConfigProperty(name = "mongodb.database")
    String databaseName;

    @Inject
    @MongoClientName("main")
    MongoClient client;

    @Inject
    MongoBsonCreator bsonCreator;

    private static final Logger LOG = Logger.getLogger(MongoDao.class);

    @Override
    public void insert(DatabaseEntry databaseEntry) {
        try {
            MongoDatabase db = getDatabase();
            Document insert = bsonCreator.insertDocument(databaseEntry);
            db.getCollection(databaseEntry.getDatabaseTableMetadata().getName()).insertOne(insert);
        }
        catch (Exception me) {
            LOG.error("Could not insert into database" + databaseEntry);
            LOG.error(me.getMessage());
            throw me;
        }
    }

    @Override
    public void delete(DatabaseEntry databaseEntry) {

    }

    @Override
    public void update(DatabaseEntry databaseEntry) {
        try {
            MongoDatabase db = getDatabase();
            DatabaseColumnEntry primary = databaseEntry.getPrimaryColumnEntry();
            if (primary == null) {
                throw new RuntimeException("Cannot update without primary key");
            }
            Bson filter = bsonCreator.getPrimaryFilter(databaseEntry);
            Bson update = bsonCreator.updateBson(databaseEntry);
            db.getCollection(databaseEntry.getDatabaseTableMetadata().getName()).updateOne(filter, update);
        }
        catch (Exception me) {
            LOG.error("Could not update database" + databaseEntry);
            LOG.error(me.getMessage());
            throw me;
        }
    }

    public void bulkWrite(List<WriteModel<Document>> bulkOperations, String collection) {
        try {
            MongoDatabase db = getDatabase();
            db.getCollection(collection).bulkWrite(bulkOperations);
        }
        catch (Exception me) {
            LOG.error("Could not do bulk operation to collection " + collection);
            LOG.error(me.getMessage());
            throw me;
        }
    }

    @Override
    public void createTable(DatabaseEntry databaseEntry) {
        DatabaseTableMetadata metadata = databaseEntry.getDatabaseTableMetadata();
        try {
            MongoDatabase db = getDatabase();
            db.createCollection(metadata.getName());
        }
        catch (Exception me) {
            LOG.error("Could not create table " + metadata);
            LOG.error(me.getMessage());
            throw me;
        }
    }

    /**
     * Since Mongo does table alteration automatically this method does not need to do anything. Due to implementing the DAO interface the method must be here even though is empty.
     * @param columns does nothing
     * @param metadata does nothing
     */
    @Override
    public void alterTable(List<DatabaseColumn> columns, DatabaseTableMetadata metadata) {
    }

    @Override
    public void dropTable(DatabaseEntry databaseEntry) {
        DatabaseTableMetadata metadata = databaseEntry.getDatabaseTableMetadata();
        try {
            MongoDatabase db = getDatabase();
            db.getCollection(metadata.getName()).drop();
        }
        catch (Exception me) {
            LOG.error("Could not drop table " + metadata);
            LOG.error(me.getMessage());
            throw me;
        }
    }

    @Override
    public void resetDatabase() {
        try {
            MongoDatabase db = getDatabase();
            db.drop();
            getDatabase();
            LOG.info("Successfully reset database");
        }
        catch (Exception me) {
            LOG.error("Could not reset database");
            LOG.error(me.getMessage());
            throw me;
        }
    }

    @Override
    public void executeStatement(String statement) {
    }

    @Override
    public void executePreparedStatement(String statement) {
    }

    @Override
    public void executeBatchStatement(List<String> statements) {
    }

    @Override
    public Instant timedInsert(DatabaseEntry databaseEntry) {
        try {
            MongoDatabase db = getDatabase();
            Document insert = bsonCreator.insertDocument(databaseEntry);
            db.getCollection(databaseEntry.getDatabaseTableMetadata().getName()).insertOne(insert);
            return Instant.now();
        }
        catch (Exception me) {
            LOG.error("Could not insert into database timed request" + databaseEntry);
            LOG.error(me.getMessage());
            throw me;
        }
    }

    private MongoDatabase getDatabase() {
        return client.getDatabase(databaseName);
    }
}
