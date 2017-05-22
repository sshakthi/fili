// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.fasterxml.jackson.databind.JsonNode;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response.Status;

/**
 * Response processor for matching partial data.
 * <p>
 * In druid version 0.9.0 or later, druid implemented a feature that returns missing intervals for a given query
 * in the header of the query response from the Broker. This information is used in addition to the features
 * supported in Partial Data V1. We validate that what we expects from broker matches what the broker actually returned.
 * <p>
 * For example, an example below shows a druid query that requests broker to return the missing intervals:
 *
 * <pre>
 * {@code
 * Content-Type: application/json
 * {
 *     "queryType": "groupBy",
 *     "dataSource": "semiAvailableTable",
 *     "granularity": "day",
 *     "dimensions": [ "line_id" ],
 *     "aggregations": [ { "type": "longSum", "name": "myMetric", "fieldName": "myMetric" } ],
 *     "intervals": [ "2016-11-21/2017-12-19" ],
 *     "context": { "uncoveredIntervalsLimit": 10 }
 * }
 * }
 * </pre>
 *
 * In "context" section, "uncoveredIntervalsLimit" is set to let druid know that we want broker to return a list of
 * intervals that are not present. The value "10" indicates to return the first 10 continuous uncovered interval. With
 * this query, Druid responses with the header:
 *
 * <pre>
 * {@code
 * Content-Type: application/json
 * 200 OK
 * Date:  Mon, 10 Apr 2017 16:24:24 GMT
 * Content-Type:  application/json
 * X-Druid-Query-Id:  92c81bed-d9e6-4242-836b-0fcd1efdee9e
 * X-Druid-Response-Context: {
 *     "uncoveredIntervals": [
 *         "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z","2016-12-25T00:00:00.000Z/2017-
 *         01-03T00:00:00.000Z","2017-01-31T00:00:00.000Z/2017-02-01T00:00:00.000Z","2017-02-
 *         08T00:00:00.000Z/2017-02-09T00:00:00.000Z","2017-02-10T00:00:00.000Z/2017-02-
 *         13T00:00:00.000Z","2017-02-16T00:00:00.000Z/2017-02-20T00:00:00.000Z","2017-02-
 *         22T00:00:00.000Z/2017-02-25T00:00:00.000Z","2017-02-26T00:00:00.000Z/2017-03-
 *         01T00:00:00.000Z","2017-03-04T00:00:00.000Z/2017-03-05T00:00:00.000Z","2017-03-
 *         08T00:00:00.000Z/2017-03-09T00:00:00.000Z"
 *     ],
 *     "uncoveredIntervalsOverflowed": true
 * }
 * Content-Encoding:  gzip
 * Vary:  Accept-Encoding, User-Agent
 * Transfer-Encoding:  chunked
 * Server:  Jetty(9.2.5.v20141112)
 * }
 * </pre>
 *
 * The flag "uncoveredIntervalsOverflowed" being true indicates that there are more uncovered intervals in addition to
 * the first 10 included. Using the "uncoveredIntervals", we can compare it to the missing intervals that we expects
 * from Partial Data V1. If "uncoveredIntervals" contains any interval that is not present in our expected
 * missing interval list, we can send back an error response indicating the mismatch in data availability before the
 * response is cached.
 */
public class PartialDataV2ResponseProcessor implements FullResponseProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(PartialDataV2ResponseProcessor.class);

    private final ResponseProcessor next;

    /**
     * Constructor.
     *
     * @param next  Next ResponseProcessor in the chain
     */
    public PartialDataV2ResponseProcessor(ResponseProcessor next) {
        this.next = next;
    }

    @Override
    public ResponseContext getResponseContext() {
        return next.getResponseContext();
    }

    @Override
    public FailureCallback getFailureCallback(DruidAggregationQuery<?> druidQuery) {
        return next.getFailureCallback(druidQuery);
    }

    @Override
    public HttpErrorCallback getErrorCallback(DruidAggregationQuery<?> druidQuery) {
        return next.getErrorCallback(druidQuery);
    }

    /**
     * If status code is 200, do the following
     *
     * <ol>
     *     <li>
     *         Extract uncoveredIntervalsOverflowed from X-Druid-Response-Context inside the JsonNode passed into
     *         PartialDataV2ResponseProcessor::processResponse, if it is true, invoke error response saying limit
     *         overflowed,
     *     </li>
     *     <li>
     *         Extract uncoveredIntervals from X-Druid-Response-Contex inside the JsonNode passed into
     *         PartialDataV2ResponseProcessor::processResponse,
     *     </li>
     *     <li>
     *         Parse both the uncoveredIntervals extracted above and allAvailableIntervals extracted from the union of
     *         all the query's datasource's availabilities from DataSourceMetadataService into SimplifiedIntervalLists,
     *     </li>
     *     <li>
     *         Compare both SimplifiedIntervalLists above, if allAvailableIntervals has any overlap with
     *         uncoveredIntervals, invoke error response indicating druid is missing some data that are we expects to
     *         exists.
     *     </li>
     * </ol>
     *
     * @param json  The json representing a druid data response
     * @param query  The query with the schema for processing this response
     * @param metadata  The LoggingContext to use
     */
    @Override
    public void processResponse(JsonNode json, DruidAggregationQuery<?> query, LoggingContext metadata) {
        validateJsonResponse(json, query);

        if (json.get("status-code").asInt() == Status.OK.getStatusCode()) {
            checkOverflow(json, query);

            SimplifiedIntervalList overlap = getUncoveredIntervalsFromResponse(json).intersect(
                    query.getDataSource().getPhysicalTable().getAvailableIntervals()
            );
            if (!overlap.isEmpty()) {
                logAndGetErrorCallback(ErrorMessageFormat.DATA_AVAILABILITY_MISMATCH.format(overlap), query);
            }
            if (next instanceof FullResponseProcessor) {
                next.processResponse(json, query, metadata);
            } else {
                next.processResponse(json.get("response"), query, metadata);
            }
        }

        next.processResponse(json, query, metadata);
    }

    /**
     * Validates JSON response object to make sure it contains all of the following information.
     * <ul>
     *     <li>X-Druid-Response-Context
     *         <ol>
     *             <li>uncoveredIntervals</li>
     *             <li>uncoveredIntervalsOverflowed</li>
     *         </ol>
     *     </li>
     *     <li>status-code</li>
     * </ul>
     *
     * @param json  The JSON response that is to be validated
     * @param query  The query with the schema for processing this response
     */
    private void validateJsonResponse(JsonNode json, DruidAggregationQuery<?> query) {
        if (!json.has("X-Druid-Response-Context")) {
            logAndGetErrorCallback("Response is missing X-Druid-Response-Context", query);
        }
        if (!json.get("X-Druid-Response-Context").has("uncoveredIntervals")) {
            logAndGetErrorCallback("Response is missing 'uncoveredIntervals' X-Druid-Response-Context", query);
        }
        if (!json.get("X-Druid-Response-Context").has("uncoveredIntervalsOverflowed")) {
            logAndGetErrorCallback(
                    "Response is missing 'uncoveredIntervalsOverflowed' X-Druid-Response-Context",
                    query
            );
        }
        if (!json.has("status-code")) {
            logAndGetErrorCallback("Response is missing response status code", query);
        }
    }

    /**
     * Checks and invokes error if the number of missing intervals are overflowed, i.e. more than the configured limit.
     *
     * @param json  The json object containing the overflow flag
     * @param query  The query with the schema for processing this response
     */
    private void checkOverflow(JsonNode json, DruidAggregationQuery<?> query) {
        if (json.get("X-Druid-Response-Context").get("uncoveredIntervalsOverflowed").asBoolean()) {
            int limit = query.getContext().getUncoveredIntervalsLimit();
            logAndGetErrorCallback(ErrorMessageFormat.TOO_MUCH_INTERVAL_MISSING.format(limit, limit), query);
        }
    }

    /**
     * Logs and gets error call back on the response with the provided error message.
     *
     * @param message  The error message passed to the logger and the exception
     * @param query  The query with the schema for processing this response
     */
    private void logAndGetErrorCallback(String message, DruidAggregationQuery<?> query) {
        LOG.error(message);
        getErrorCallback(query).dispatch(
                Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                "The server encountered an unexpected condition which prevented it from fulfilling the request.",
                message);
    }

    /**
     * Returns uncovered intervals from Druid response in a SimplifiedIntervalList.
     *
     * @param json the JSON object containing the list of uncovered intervals, for example
     * <pre>
     * {@code
     * X-Druid-Response-Context: {
     *     "uncoveredIntervals": [
     *         "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z","2016-12-25T00:00:00.000Z/2017-
     *         01-03T00:00:00.000Z","2017-01-31T00:00:00.000Z/2017-02-01T00:00:00.000Z","2017-02-
     *         08T00:00:00.000Z/2017-02-09T00:00:00.000Z","2017-02-10T00:00:00.000Z/2017-02-
     *         13T00:00:00.000Z","2017-02-16T00:00:00.000Z/2017-02-20T00:00:00.000Z","2017-02-
     *         22T00:00:00.000Z/2017-02-25T00:00:00.000Z","2017-02-26T00:00:00.000Z/2017-03-
     *         01T00:00:00.000Z","2017-03-04T00:00:00.000Z/2017-03-05T00:00:00.000Z","2017-03-
     *         08T00:00:00.000Z/2017-03-09T00:00:00.000Z"
     *     ],
     *     "uncoveredIntervalsOverflowed": true
     * }
     * }
     * </pre>
     *
     * @return uncovered intervals in a SimplifiedIntervalList.
     */
    private static SimplifiedIntervalList getUncoveredIntervalsFromResponse(JsonNode json) {
        List<Interval> intervals = new ArrayList<>();
        for (JsonNode jsonNode : json.get("X-Druid-Response-Context").get("uncoveredIntervals")) {
            intervals.add(new Interval(jsonNode.asText()));
        }
        return new SimplifiedIntervalList(intervals);
    }
}
