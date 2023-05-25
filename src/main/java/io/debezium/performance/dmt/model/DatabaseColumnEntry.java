/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.model;

import java.util.Objects;

public record DatabaseColumnEntry(String value, String columnName, String dataType) {

    @Override
    public String toString() {
        return "DatabaseColumnEntry{" +
                "value='" + value + '\'' +
                ", columnName='" + columnName + '\'' +
                ", dataType='" + dataType + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatabaseColumnEntry entry = (DatabaseColumnEntry) o;
        return Objects.equals(value, entry.value) && Objects.equals(columnName, entry.columnName) && Objects.equals(dataType, entry.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, columnName, dataType);
    }
}