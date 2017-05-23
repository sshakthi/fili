// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client.impl;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.function.Function;

/**
 * A strategy that takes a JSON Node built from a Response, nests it and makes it a peer with the Response headers.
 */
public class HeaderNestingJsonBuilderStrategy implements Function<Response, JsonNode> {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final boolean DRUID_ETAG_CACHE_ENABLED = SYSTEM_CONFIG.getBooleanProperty(
            SYSTEM_CONFIG.getPackageVariableName("druid_etag_cache_enabled"),
            true
    );

    private final Function<Response, JsonNode> baseStrategy;

    /**
     * Constructor.
     *
     * @param baseStrategy  strategy to do initial JSON Node construction
     */
    public HeaderNestingJsonBuilderStrategy(Function<Response, JsonNode> baseStrategy) {
        this.baseStrategy = baseStrategy;
    }

    @Override
    public JsonNode apply(Response response) {
        MappingJsonFactory mappingJsonFactory = new MappingJsonFactory();
        ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
        objectNode.set("response", baseStrategy.apply(response));
        try {
            objectNode.set(
                    "X-Druid-Response-Context",
                    mappingJsonFactory
                            .createParser(response.getHeader("X-Druid-Response-Context"))
                            .readValueAsTree()
            );
            objectNode.set(
                    "status-code",
                    mappingJsonFactory.createParser(String.valueOf(response.getStatusCode())).readValueAsTree()
            );
            if (DRUID_ETAG_CACHE_ENABLED) {
                objectNode.set(
                        "If-None-Match",
                        mappingJsonFactory.createParser(response.getHeader("If-None-Match"))
                        .readValueAsTree()
                );
            }
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
        return objectNode;
    }
}
