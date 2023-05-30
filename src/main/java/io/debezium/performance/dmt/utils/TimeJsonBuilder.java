package io.debezium.performance.dmt.utils;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.time.Instant;

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
