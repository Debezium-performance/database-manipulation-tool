/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.utils;

import javax.json.JsonException;
import javax.json.JsonObject;

public interface DataParser<T> {
    T parse(JsonObject inputJsonObject) throws JsonException;
}
