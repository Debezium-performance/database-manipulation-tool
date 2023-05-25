/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.dmt.service;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.JsonObject;

import org.jboss.logging.Logger;

import io.debezium.performance.dmt.dao.DaoManager;
import io.debezium.performance.dmt.model.Database;
import io.debezium.performance.dmt.utils.ResponseJsonBuilder;

@ApplicationScoped
public class UtilityService {

    @Inject
    Database database;

    @Inject
    DaoManager daoManager;

    private static final Logger LOG = Logger.getLogger(UtilityService.class);

    public JsonObject getAllEntries() {
        return new ResponseJsonBuilder(database)
                .addDatabases(daoManager.getEnabledDbsNames())
                .build();
    }
}
