// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

/**
 * Response processor for that extracts header information from Druid response and put the information in our own
 * response.
 * <p>
 * This is a "Full" response process in a sense that it extracts and incorporates header information.
 */
public interface FullResponseProcessor extends ResponseProcessor {
}
