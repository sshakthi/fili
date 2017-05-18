// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors

import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.QueryContext
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import org.joda.time.Interval

import spock.lang.Specification

class DruidPartialDataResponseProcessorSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    DruidAggregationQuery druidAggregationQuery
    ResponseContext responseContext
    FailureCallback failureCallback
    HttpErrorCallback httpErrorCallback
    ResponseProcessor next

    QueryContext queryContext

    DruidPartialDataResponseProcessor druidPartialDataResponseProcessor

    def setup() {
        druidAggregationQuery = Mock(DruidAggregationQuery)
        responseContext = Mock(ResponseContext)
        failureCallback = Mock(FailureCallback)
        httpErrorCallback = Mock(HttpErrorCallback)
        next = Mock(ResponseProcessor)
        next.getResponseContext() >> responseContext
        next.getFailureCallback(druidAggregationQuery) >> failureCallback
        next.getErrorCallback(druidAggregationQuery) >> httpErrorCallback

        queryContext = Mock(QueryContext)
        queryContext.getUncoveredIntervalsLimit() >> 10
        druidAggregationQuery.getContext() >> queryContext

        druidPartialDataResponseProcessor = new DruidPartialDataResponseProcessor(next)
    }

    def "Test constructor"() {
        expect:
        druidPartialDataResponseProcessor.next == next;
    }

    def "getOverlap returns intersection between Druid intervals and Fili intervals in case of #caseDescription"() {
        given:
        JsonNode json = MAPPER.readTree(
                jsonInString
                        .replace(" ", "")
                        .replace("\n", "")
        )

        DataSource dataSource = Mock(DataSource)
        ConstrainedTable constrainedTable = Mock(ConstrainedTable)

        constrainedTable.getAvailableIntervals() >> new SimplifiedIntervalList(
                availableIntervals.collect{it -> new Interval(it)}
        )
        dataSource.getPhysicalTable() >> constrainedTable
        druidAggregationQuery.getDataSource() >> dataSource


        where:
        jsonInString | availableIntervals | expected | caseDescription
        '''
        {
            "response": [{"k1":"v1"}],
            "X-Druid-Response-Context": {
                "uncoveredIntervals": [
                    "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z",
                    "2016-12-25T00:00:00.000Z/2017-01-03T00:00:00.000Z"
                ],
                "uncoveredIntervalsOverflowed": true
            },
            "status-code": 200
        }
        ''' |
                ["2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z"] |
                ["2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z"] | "overlapping"
        '''
        {
            "response": [{"k1":"v1"}],
            "X-Druid-Response-Context": {
                "uncoveredIntervals": [
                    "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z",
                    "2016-12-25T00:00:00.000Z/2017-01-03T00:00:00.000Z"
                ],
                "uncoveredIntervalsOverflowed": true
            },
            "status-code": 200
        }
        ''' |
                ["2019-11-22T00:00:00.000Z/2019-12-18T00:00:00.000Z"] |
                [] | "no overlapping"
        '''
        {
            "response": [{"k1":"v1"}],
            "X-Druid-Response-Context": {
                "uncoveredIntervals": [
                    "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z",
                    "2016-12-25T00:00:00.000Z/2017-01-03T00:00:00.000Z"
                ],
                "uncoveredIntervalsOverflowed": true
            },
            "status-code": 200
        }
        ''' |
                [] |
                [] | "non-overlapping (Fili has no emtpy intervals)"
        '''
        {
            "response": [{"k1":"v1"}],
            "X-Druid-Response-Context": {
                "uncoveredIntervals": [
                    "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z",
                    "2016-12-25T00:00:00.000Z/2017-01-03T00:00:00.000Z"
                ],
                "uncoveredIntervalsOverflowed": true
            },
            "status-code": 200
        }
        ''' |
                ["2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z", "2016-12-25T00:00:00.000Z/2017-01-03T00:00:00.000Z"] |
                ["2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z", "2016-12-25T00:00:00.000Z/2017-01-03T00:00:00.000Z"] | "being equal"
    }

    def "checkOverflow recognizes interval overflow correctly"() {
        given:
        JsonNode json = MAPPER.readTree(
                '''
                {
                    "response": [{"k1":"v1"}],
                    "X-Druid-Response-Context": {
                        "uncoveredIntervals": [
                            "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z",
                            "2016-12-25T00:00:00.000Z/2017-01-03T00:00:00.000Z"
                        ],
                        "uncoveredIntervalsOverflowed": true
                    },
                    "status-code": 200
                }
                '''.replace(" ", "").replace("\n", "")
        )

        when:
        druidPartialDataResponseProcessor.checkOverflow(json, druidAggregationQuery)

        then:
        1 * httpErrorCallback.dispatch(
                500,
                'The server encountered an unexpected condition which prevented it from fulfilling the ' +
                'request.',
                'Query is returning more than the configured limit of \'10\' missing intervals. ' +
                'There may be a problem with your data.')
    }

    def "processResponse logs and invokes error callback on data availability mismatch"() {
        given:
        JsonNode json = MAPPER.readTree(
                '''
                {
                    "response": [{"k1":"v1"}],
                    "X-Druid-Response-Context": {
                        "uncoveredIntervals": [
                            "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z",
                            "2016-12-25T00:00:00.000Z/2017-01-03T00:00:00.000Z"
                        ],
                        "uncoveredIntervalsOverflowed": false
                    },
                    "status-code": 200
                }
                '''.replace(" ", "").replace("\n", "")
        )

        DataSource dataSource = Mock(DataSource)
        ConstrainedTable constrainedTable = Mock(ConstrainedTable)

        constrainedTable.getAvailableIntervals() >> new SimplifiedIntervalList(
                [new Interval("2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z")]
        )
        dataSource.getPhysicalTable() >> constrainedTable
        druidAggregationQuery.getDataSource() >> dataSource

        when:
        druidPartialDataResponseProcessor.processResponse(json, druidAggregationQuery, Mock(LoggingContext))

        then:
        1 * httpErrorCallback.dispatch(
                500,
                'The server encountered an unexpected condition which prevented it from fulfilling the request.', 'Data availability expectation does not match with actual query result obtained from druid for the following intervals [2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z] where druid does not have data'
        )

    }

    def "validateJsonResponse recognizes missing component"() {
        given:
        ArrayNode arrayNode = Mock(ArrayNode)
        JsonNode druidResponseContext = Mock(JsonNode)
        JsonNode json = Mock(JsonNode)
        json.get(FullResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName()) >> druidResponseContext

        when:
        druidPartialDataResponseProcessor.validateJsonResponse(arrayNode, druidAggregationQuery)
        then:
        1 * httpErrorCallback.dispatch(
                500,
                'The server encountered an unexpected condition which prevented it from fulfilling the ' +
                'request.',
                'Response is missing X-Druid-Response-Context and status code'
        )

        // missing X-Druid-Response-Context
        when:
        json.has(FullResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName()) >> false
        druidPartialDataResponseProcessor.validateJsonResponse(json, druidAggregationQuery)
        then:
        1 * httpErrorCallback.dispatch(
                500,
                'The server encountered an unexpected condition which prevented it from fulfilling the ' +
                'request.',
                'Response is missing X-Druid-Response-Context'
        )

        // missing X-Druid-Response-Context.uncoveredIntervals
        when:
        json.has(FullResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName()) >> true
        druidResponseContext.has(FullResponseContentKeys.UNCOVERED_INTERVALS.getName()) >> false
        druidPartialDataResponseProcessor.validateJsonResponse(json, druidAggregationQuery)
        then:
        1 * httpErrorCallback.dispatch(
                500,
                'The server encountered an unexpected condition which prevented it from fulfilling the ' +
                'request.',
                "Response is missing 'uncoveredIntervals' from X-Druid-Response-Context header"
        )

        // missing X-Druid-Response-Context.uncoveredIntervalsOverflowed
        when:
        json.has(FullResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName()) >> true
        druidResponseContext.has(FullResponseContentKeys.UNCOVERED_INTERVALS.getName()) >> true
        druidResponseContext.has(FullResponseContentKeys.UNCOVERED_INTERVALS_OVERFLOWED.getName()) >> false
        druidPartialDataResponseProcessor.validateJsonResponse(json, druidAggregationQuery)
        then:
        1 * httpErrorCallback.dispatch(
                500,
                'The server encountered an unexpected condition which prevented it from fulfilling the ' +
                'request.',
                "Response is missing 'uncoveredIntervalsOverflowed' from X-Druid-Response-Context header"
        )

        // missing status code
        when:
        json.has(FullResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName()) >> true
        druidResponseContext.has(FullResponseContentKeys.UNCOVERED_INTERVALS.getName()) >> true
        druidResponseContext.has(FullResponseContentKeys.UNCOVERED_INTERVALS_OVERFLOWED.getName()) >> true
        json.has(FullResponseContentKeys.STATUS_CODE.getName()) >> false
        druidPartialDataResponseProcessor.validateJsonResponse(json, druidAggregationQuery)
        then:
        1 * httpErrorCallback.dispatch(
                500,
                'The server encountered an unexpected condition which prevented it from fulfilling the ' +
                'request.',
                "Response is missing response status code"
        )
    }
}