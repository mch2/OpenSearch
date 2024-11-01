
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request to delete one or more PIT search contexts based on IDs.
 *
 * @opensearch.api
 */
@ExperimentalApi()
public class JoinRequest extends ActionRequest {

    public SearchRequest getLeftIndex() {
        return leftIndex;
    }

    public SearchRequest getRightIndex() {
        return rightIndex;
    }

    public String getJoinField() {
        return joinField;
    }

    private final SearchRequest leftIndex;
    private final SearchRequest rightIndex;
    private final String joinField;

    public JoinRequest(StreamInput in) throws IOException {
        super(in);
        leftIndex = new SearchRequest(in);
        rightIndex = new SearchRequest(in);
        joinField = in.readString();
    }

    public JoinRequest(SearchRequest left, SearchRequest right, String joinField) {
        this.leftIndex = left;
        this.rightIndex = right;
        this.joinField = joinField;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (leftIndex == null || rightIndex == null || joinField == null) {
            validationException = addValidationError("Get it together man", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        leftIndex.writeTo(out);
        rightIndex.writeTo(out);
        out.writeString(joinField);
    }
}
