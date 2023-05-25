/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.dataSource;

import java.sql.Connection;
import java.sql.SQLException;

public interface DataSourceWrapper {
    Connection getConnection() throws SQLException;
}
