/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.dao;

import java.util.List;

import io.debezium.DatabaseColumn;
import io.debezium.DatabaseEntry;
import io.debezium.DatabaseTableMetadata;

public interface Dao {

    void insert(DatabaseEntry databaseEntry);

    void delete(DatabaseEntry databaseEntry);

    void update(DatabaseEntry databaseEntry);

    void createTable(DatabaseEntry databaseEntry);

    void alterTable(List<DatabaseColumn> columns, DatabaseTableMetadata metadata);

    void dropTable(DatabaseEntry databaseEntry);

    void resetDatabase();

}
