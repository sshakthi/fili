// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors

import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
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
        druidPartialDataResponseProcessor = new DruidPartialDataResponseProcessor(next)
    }

    def "Test constructor"() {
        expect:
        druidPartialDataResponseProcessor.next == next;
    }

    def "Test proxy calls"() {
        when:
        druidPartialDataResponseProcessor.getResponseContext()
        druidPartialDataResponseProcessor.getErrorCallback(druidAggregationQuery)
        druidPartialDataResponseProcessor.getFailureCallback(druidAggregationQuery)

        then:
        1 * next.getResponseContext() >> responseContext
        1 * next.getFailureCallback(druidAggregationQuery) >> failureCallback
        1 * next.getErrorCallback(druidAggregationQuery) >> httpErrorCallback
    }

    def "getOverlap returns intersection between Druid intervals and Fili intervals"() {
        given:
        JsonNode json = MAPPER.readTree(
                jsonInString
                        .replace(" ", "")
                        .replace("\n", "")
        )

        DataSource dataSource = Mock(DataSource)
        ConstrainedTable constrainedTable = Mock(ConstrainedTable)

        constrainedTable.getAvailableIntervals() >> new SimplifiedIntervalList(availableIntervals.collect{it -> new Interval(it)})
        dataSource.getPhysicalTable() >> constrainedTable
        druidAggregationQuery.getDataSource() >> dataSource


        where:
        jsonInString | availableIntervals | expected
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
        ''' | ["2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z"] | ["2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z"]
    }
}
