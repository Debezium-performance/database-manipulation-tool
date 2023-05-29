package io.debezium.performance.dmt.utils;

import io.debezium.performance.dmt.schema.DatabaseColumnEntry;
import io.debezium.performance.dmt.schema.DatabaseEntry;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection(targets = { DatabaseEntry.class, DatabaseColumnEntry.class })
public class ReflectionConfiguration {
}
