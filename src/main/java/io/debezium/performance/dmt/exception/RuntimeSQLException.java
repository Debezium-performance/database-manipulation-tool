/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.exception;

public class RuntimeSQLException extends RuntimeException {
    public RuntimeSQLException(Throwable cause) {
        super(cause);
    }
}
