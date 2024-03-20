/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.parser;

import javax.json.JsonException;

public interface DataParser<T, E> {
    T parse(E inputObject) throws JsonException;
}
