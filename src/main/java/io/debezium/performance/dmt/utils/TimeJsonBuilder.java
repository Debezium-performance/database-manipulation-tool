/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.performance.dmt.utils;

import java.time.Instant;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class TimeJsonBuilder {
    JsonObjectBuilder builder;

    public TimeJsonBuilder() {
        builder = Json.createObjectBuilder();
    }

    public TimeJsonBuilder addDbTimestamp(String database, Instant time) {
        builder.add(database, time.toString());
        return this;
    }

    public JsonObject build() {
        return builder.build();
    }
}
