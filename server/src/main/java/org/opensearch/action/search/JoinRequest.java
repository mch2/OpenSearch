
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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Join Request
 *
 * @opensearch.experimental
 */
@ExperimentalApi()
public class JoinRequest extends ActionRequest {

    private final SearchRequest leftIndex;
    private final SearchRequest rightIndex;
    private final String joinField;
    private final String leftAlias;
    private final String rightAlias;
    private final boolean getHits;

    public JoinRequest(StreamInput in) throws IOException {
        super(in);
        leftIndex = new SearchRequest(in);
        rightIndex = new SearchRequest(in);
        joinField = in.readString();
        leftAlias = in.readString();
        rightAlias = in.readString();
        this.getHits = in.readBoolean();
    }

    public JoinRequest(SearchRequest left, SearchRequest right, String joinField, String leftAlias, String rightAlias) {
        this(left, right, joinField, rightAlias, leftAlias, false);
    }

    public JoinRequest(SearchRequest left, SearchRequest right, String joinField, String leftAlias, String rightAlias, boolean getHits) {
        this.leftIndex = left;
        this.rightIndex = right;
        this.joinField = joinField;
        this.leftAlias = leftAlias;
        this.rightAlias = rightAlias;
        this.getHits = getHits;
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
        out.writeString(leftAlias);
        out.writeString(rightAlias);
        out.writeBoolean(getHits);
    }

    public SearchRequest getLeftIndex() {
        return leftIndex;
    }

    public SearchRequest getRightIndex() {
        return rightIndex;
    }

    public String getJoinField() {
        return joinField;
    }

    public String getLeftAlias() {
        return leftAlias;
    }

    public String getRightAlias() {
        return rightAlias;
    }

    public boolean isGetHits() {
        return getHits;
    }
}
