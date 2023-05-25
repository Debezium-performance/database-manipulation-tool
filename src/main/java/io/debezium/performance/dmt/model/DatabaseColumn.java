/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.model;

import java.util.Objects;

public class DatabaseColumn {
    private String name;
    private String dataType;
    private boolean primary;

    public DatabaseColumn(String name, String dataType, boolean primary) {
        this.name = name;
        this.dataType = dataType;
        this.primary = primary;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public void setPrimary(boolean primary) {
        this.primary = primary;
    }

    public boolean isPrimary() {
        return primary;
    }

    @Override
    public String toString() {
        return "DatabaseColumn{" +
                "name='" + name + '\'' +
                ", dataType='" + dataType + '\'' +
                ", primary=" + primary +
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
        DatabaseColumn that = (DatabaseColumn) o;
        return primary == that.primary && Objects.equals(name, that.name) && Objects.equals(dataType, that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, primary);
    }
}
