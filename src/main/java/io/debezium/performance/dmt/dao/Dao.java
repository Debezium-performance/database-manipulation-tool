/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.dao;

import java.util.List;

import io.debezium.performance.dmt.schema.DatabaseColumn;
import io.debezium.performance.dmt.schema.DatabaseEntry;
import io.debezium.performance.dmt.schema.DatabaseTableMetadata;

public interface Dao {

    void insert(DatabaseEntry databaseEntry);

    void delete(DatabaseEntry databaseEntry);

    void update(DatabaseEntry databaseEntry);

    void createTable(DatabaseEntry databaseEntry);

    void alterTable(List<DatabaseColumn> columns, DatabaseTableMetadata metadata);

    void dropTable(DatabaseEntry databaseEntry);

    void resetDatabase();

}
