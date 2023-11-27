package io.debezium.performance.dmt.generator;

import io.debezium.performance.dmt.parser.DmtSchemaParser;
import io.debezium.performance.dmt.model.DatabaseEntry;
import io.debezium.performance.load.data.builder.AviationDataBuilder;
import io.debezium.performance.load.data.builder.ByteDataBuilder;
import io.debezium.performance.load.data.builder.RequestBuilder;
import io.debezium.performance.load.scenarios.builder.ConstantScenarioBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class Generator {
    @Inject
    DmtSchemaParser parser;
    public List<DatabaseEntry> generateAviationBatch(int count, int maxRows) {
        RequestBuilder<ConstantScenarioBuilder> requestBuilder
                = new RequestBuilder<>(new AviationDataBuilder(), new ConstantScenarioBuilder(1, 1));

        List<io.debezium.performance.dmt.schema.DatabaseEntry> entries = requestBuilder
                .setRequestCount(count)
                .setMaxRows(maxRows)
                .buildPlain();

        return entries.stream().map(parser::parse).toList();
    }

    public List<DatabaseEntry> generateByteBatch(int count, int maxRows, int messageSize) {
        RequestBuilder<ConstantScenarioBuilder> requestBuilder = new RequestBuilder<>(new ByteDataBuilder(1), new ConstantScenarioBuilder(1,1));
        AtomicInteger i = new AtomicInteger(1);
        String message = "a".repeat(messageSize);
        List<io.debezium.performance.dmt.schema.DatabaseEntry> entries = requestBuilder
            .setRequestCount(count)
            .setMaxRows(maxRows)
            .buildPlain();
           entries.forEach(databaseEntry -> {
                databaseEntry.setPrimary("_id");
                databaseEntry.getColumnEntries().forEach(databaseColumnEntry -> {
                    if (databaseColumnEntry.getColumnName().equals("id")){
                        databaseColumnEntry.setColumnName("_id");
                    }
                    if (databaseColumnEntry.getColumnName().equals("payload")) {
                        databaseColumnEntry.setValue(i.getAndIncrement() + message);
                    }
                });
           });
        return entries.stream().map(parser::parse).toList();
    }
}
