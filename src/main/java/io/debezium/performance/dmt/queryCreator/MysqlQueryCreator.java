/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.queryCreator;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public final class MysqlQueryCreator extends AbstractBasicQueryCreator {

    public MysqlQueryCreator() {
    }

    @Override
    public String dropDatabase(String schema) {
        String query = "DROP SCHEMA IF EXISTS " + schema;
        LOG.debug("Created DROP DATABASE query: " + query);
        return query;
    }

    @Override
    public String createDatabase(String schema) {
        String query = "CREATE SCHEMA " + schema;
        LOG.debug("Created CREATE DATABASE query: " + query);
        return query;
    }

    public String useDatabase(String schema) {
        String query = "USE " + schema;
        LOG.debug("Created USE DATABASE query: " + query);
        return query;
    }
}
