/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.performance.dmt.utils;

import io.debezium.performance.dmt.schema.DatabaseColumnEntry;
import io.debezium.performance.dmt.schema.DatabaseEntry;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection(targets = { DatabaseEntry.class, DatabaseColumnEntry.class })
public class ReflectionConfiguration {
}
