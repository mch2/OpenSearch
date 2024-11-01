/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.stream.OSTicket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Response class for delete pits flow which clears the point in time search contexts
 *
 * @opensearch.api
 */
@PublicApi(since = "2.3.0")
public class JoinResponse extends ActionResponse {

    private final OSTicket ticket;

    public OSTicket getTicket() {
        return ticket;
    }

    public JoinResponse(OSTicket ticket) {
        this.ticket = ticket;
    }

    public JoinResponse(StreamInput in) throws IOException {
        super(in);
        this.ticket = new OSTicket(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
       ticket.writeTo(out);
    }

    @Override
    public String toString() {
        return "JoinResponse{" +
            "ticket=" + ticket +
            '}';
    }
}
