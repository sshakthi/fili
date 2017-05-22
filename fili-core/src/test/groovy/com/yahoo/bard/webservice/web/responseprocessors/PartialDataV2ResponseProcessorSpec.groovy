// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors

import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import org.joda.time.Interval

import spock.lang.Specification

class PartialDataV2ResponseProcessorSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    DruidAggregationQuery druidAggregationQuery
    ResponseContext responseContext
    FailureCallback failureCallback
    HttpErrorCallback httpErrorCallback
    ResponseProcessor next
    PartialDataV2ResponseProcessor partialDataV2ResponseProcessor

    def setup() {
        druidAggregationQuery = Mock(DruidAggregationQuery)
        responseContext = Mock(ResponseContext)
        failureCallback = Mock(FailureCallback)
        httpErrorCallback = Mock(HttpErrorCallback)
        next = Mock(ResponseProcessor)
        next.getResponseContext() >> responseContext
        next.getFailureCallback(druidAggregationQuery) >> failureCallback
        next.getErrorCallback(druidAggregationQuery) >> httpErrorCallback
        partialDataV2ResponseProcessor = new PartialDataV2ResponseProcessor(next)
    }

    def "Test constructor"() {
        expect:
        partialDataV2ResponseProcessor.next == next;
    }

    def "Test proxy calls"() {
        when:
        partialDataV2ResponseProcessor.getResponseContext()
        partialDataV2ResponseProcessor.getErrorCallback(druidAggregationQuery)
        partialDataV2ResponseProcessor.getFailureCallback(druidAggregationQuery)

        then:
        1 * next.getResponseContext() >> responseContext
        1 * next.getFailureCallback(druidAggregationQuery) >> failureCallback
        1 * next.getErrorCallback(druidAggregationQuery) >> httpErrorCallback
    }

    def "getUncoveredIntervalsFromResponse returns all uncovered intervals in SimplifiedList"() {
        given:
        JsonNode json = MAPPER.readTree(
                jsonInString
                        .replace(" ", "")
                        .replace("\n", "")
        )

        expect:
        partialDataV2ResponseProcessor.getUncoveredIntervalsFromResponse(json) == new SimplifiedIntervalList(
                expected.collect{it -> new Interval(it)}
        )

        where:
        jsonInString | expected
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
        ''' | ["2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z", "2016-12-25T00:00:00.000Z/2017-01-03T00:00:00.000Z"]
    }
}
