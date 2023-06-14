package io.debezium.performance.dmt.generator;

import io.debezium.performance.dmt.parser.DmtSchemaParser;
import io.debezium.performance.dmt.model.DatabaseEntry;
import io.debezium.performance.load.data.builder.AviationDataBuilder;
import io.debezium.performance.load.data.builder.RequestBuilder;
import io.debezium.performance.load.scenarios.builder.ConstantScenarioBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

@ApplicationScoped
public class Generator {
    @Inject
    DmtSchemaParser parser;
    public List<DatabaseEntry> generateBatch(int count, int maxRows) {
        RequestBuilder<ConstantScenarioBuilder> requestBuilder
                = new RequestBuilder<>(new AviationDataBuilder(), new ConstantScenarioBuilder(1, 1));

        List<io.debezium.performance.dmt.schema.DatabaseEntry> entries = requestBuilder
                .setEndpoint("http://10.40.3.179:8080/Main/CreateTableAndUpsert")
                .setRequestCount(count)
                .setMaxRows(maxRows)
                .buildPlain();

        return entries.stream().map(parser::parse).toList();
    }
}
