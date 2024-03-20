package io.debezium.performance.dmt.parser;

import io.debezium.performance.dmt.model.DatabaseEntry;

import javax.enterprise.context.ApplicationScoped;

import static io.debezium.performance.dmt.parser.ParseUtils.fromSchema;

@ApplicationScoped
public class DmtSchemaParser implements DataParser<DatabaseEntry, io.debezium.performance.dmt.schema.DatabaseEntry>{
    @Override
    public DatabaseEntry parse(io.debezium.performance.dmt.schema.DatabaseEntry inputObject) {
        return fromSchema(inputObject);
    }
}
