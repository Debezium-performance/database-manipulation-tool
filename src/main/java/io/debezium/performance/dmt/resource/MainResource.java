/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.resource;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.debezium.performance.dmt.model.DatabaseColumn;
import io.debezium.performance.dmt.model.DatabaseColumnEntry;
import io.debezium.performance.dmt.model.DatabaseTableMetadata;
import io.debezium.performance.load.data.builder.AviationDataBuilder;
import io.debezium.performance.load.data.builder.RequestBuilder;
import io.debezium.performance.load.scenarios.builder.ConstantScenarioBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.debezium.performance.dmt.model.DatabaseEntry;
import io.debezium.performance.dmt.service.MainService;
import io.debezium.performance.dmt.utils.DmtSchemaParser;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.ArrayList;
import java.util.List;

@Path("Main")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
@RegisterForReflection
public class MainResource {

    @Inject
    MainService mainService;

    @Inject
    DmtSchemaParser parser;

    @ConfigProperty(name = "onstart.reset.database", defaultValue = "false")
    boolean resetDatabase;

    private static final Logger LOG = Logger.getLogger(MainResource.class);

    @Path("Insert")
    @POST
    public Response insert(JsonObject inputJsonObj) {
        LOG.debug("Received INSERT request");
        try {
            DatabaseEntry dbEntity = parser.parse(inputJsonObj.toString());
            mainService.insert(dbEntity);
            return Response.ok().build();

        }
        catch (Exception ex) {
            return Response.noContent().status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @Path("CreateTable")
    @POST
    public Response createTable(JsonObject inputJsonObj) {
        LOG.debug("Received CREATE TABLE IF DOES NOT EXIST or ALTER TABLE IF EXISTS request");
        try {
            DatabaseEntry dbEntity = parser.parse(inputJsonObj.toString());
            mainService.createTable(dbEntity);
            return Response.ok().build();

        }
        catch (Exception ex) {
            return Response.noContent().status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @Path("CreateTableAndUpsert")
    @POST
    public Response createTableAndUpsert(String inputJsonString) {
        // LOG.debug("Received CREATE TABLE if does not exist and UPSERT request");
        try {
            DatabaseEntry dbEntity = parser.parse(inputJsonString);
            mainService.upsert(dbEntity);
            return Response.ok().build();
        }
        catch (Exception ex) {
            return Response.noContent().status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @Path("DropTable")
    @DELETE
    public Response dropTable(JsonObject inputJsonObj) {
        LOG.debug("Received DROP TABLE request");
        try {
            DatabaseEntry dbEntity = parser.parse(inputJsonObj.toString());
            mainService.dropTable(dbEntity);
            return Response.ok().build();
        }
        catch (Exception ex) {
            return Response.noContent().status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @Path("ResetDatabase")
    @GET
    public Response resetDatabase() {
        LOG.debug("Received RESET DATABASE request");
        try {
            mainService.resetDatabase();
            return Response.ok().build();
        }
        catch (Exception ex) {
            return Response.noContent().status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @Path("TimedInsert")
    @POST
    public Response timedInsert(JsonObject inputJsonObj) {
        LOG.debug("Received TIMED INSERT request");
        try {
            DatabaseEntry dbEntity = parser.parse(inputJsonObj.toString());
            JsonObject obj = mainService.timedInsert(dbEntity);
            LOG.debug("Responded to TIMED INSERT request");
            return Response.ok().entity(obj.toString()).build();
        }
        catch (Exception ex) {
            LOG.debug("Could not do timed insert");
            return Response.noContent().status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @Path("Generator")
    @GET
    public Response generatorUpsert() {
        RequestBuilder<ConstantScenarioBuilder> requestBuilder
                = new RequestBuilder<>(new AviationDataBuilder(), new ConstantScenarioBuilder(1, 1));

        List<io.debezium.performance.dmt.schema.DatabaseEntry> entries = requestBuilder
                .setEndpoint("http://10.40.3.179:8080/Main/CreateTableAndUpsert")
                .setRequestCount(10000)
                .setMaxRows(20)
                .buildPlain();

        List<DatabaseEntry> list = entries.stream().map(this::parse).toList();
        for (DatabaseEntry entry : list) {
            mainService.upsert(entry);
        }
        long start = System.currentTimeMillis();
        mainService.executeBatch();
        long result = System.currentTimeMillis() - start;
        LOG.info("Result time is " + result);
        return Response.ok().build();
    }

    void onStart(@Observes StartupEvent ev) {
        if (resetDatabase) {
            LOG.info("Restarting database on startup");
            mainService.resetDatabase();
        }
    }

    private DatabaseEntry parse(io.debezium.performance.dmt.schema.DatabaseEntry databaseEntryInput) throws JsonException {
        DatabaseEntry databaseEntry;
        List<DatabaseColumnEntry> entries = new ArrayList<>();
        DatabaseTableMetadata table = new DatabaseTableMetadata();

        table.setName(databaseEntryInput.getName());
        String primary = databaseEntryInput.getPrimary();

        for (var databaseColumnEntry : databaseEntryInput.getColumnEntries()) {
            DatabaseColumnEntry entry = new DatabaseColumnEntry(databaseColumnEntry.value(), databaseColumnEntry.columnName(), databaseColumnEntry.dataType());
            entries.add(entry);
            table.addColumn(new DatabaseColumn(entry.columnName(), entry.dataType(), primary.equals(entry.columnName())));
        }
        databaseEntry = new DatabaseEntry(entries, table);
        return databaseEntry;
    }
}
