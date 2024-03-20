/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.parser;

import javax.enterprise.context.ApplicationScoped;
import javax.json.JsonException;
import javax.json.JsonObject;

import org.jboss.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.performance.dmt.model.DatabaseEntry;

import static io.debezium.performance.dmt.parser.ParseUtils.fromSchema;

@ApplicationScoped
public class DmtSchemaJsonParser implements DataParser<DatabaseEntry, JsonObject> {

    private static final Logger LOG = Logger.getLogger(DmtSchemaJsonParser.class);

    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public DatabaseEntry parse(JsonObject inputObject) throws JsonException {
        io.debezium.performance.dmt.schema.DatabaseEntry schemaEntry;
        try {
            schemaEntry = objectMapper.readValue(inputObject.toString(), io.debezium.performance.dmt.schema.DatabaseEntry.class);
        }
        catch (Exception ex) {
            LOG.error("Could not parse DatabaseEntity from Json object " + inputObject);
            LOG.error(ex.getMessage());
            throw new JsonException("Could not parse DatabaseEntity from Json object", ex.getCause());
        }
        return fromSchema(schemaEntry);
    }
}
