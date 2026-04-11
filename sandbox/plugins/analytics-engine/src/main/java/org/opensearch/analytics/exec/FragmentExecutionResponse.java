/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Transport response carrying field names and result rows from a single shard execution.
 * Each cell value is serialized via {@link StreamOutput#writeGenericValue(Object)} /
 * {@link StreamInput#readGenericValue()}, which handle common Java types
 * (String, Long, Double, Integer, null, byte[], etc.).
 */
public class FragmentExecutionResponse extends ActionResponse {

    private final List<String> fieldNames;
    private final List<Object[]> rows;
    private final Map<String, String> metadata; // null for row data, non-null for metadata

    /** Row data constructor — metadata defaults to null. */
    public FragmentExecutionResponse(List<String> fieldNames, List<Object[]> rows) {
        this.fieldNames = fieldNames;
        this.rows = rows;
        this.metadata = null;
    }

    /** Metadata-only constructor — empty rows. */
    public FragmentExecutionResponse(Map<String, String> metadata) {
        this.fieldNames = List.of();
        this.rows = List.of();
        this.metadata = metadata;
    }

    /**
     * Deserialization constructor. Reads a boolean flag first: if true, reads metadata map;
     * otherwise reads row data (fieldNames + rows).
     */
    public FragmentExecutionResponse(StreamInput in) throws IOException {
        super(in);
        boolean hasMetadata = in.readBoolean();
        if (hasMetadata) {
            this.metadata = in.readMap(StreamInput::readString, StreamInput::readString);
            this.fieldNames = List.of();
            this.rows = List.of();
        } else {
            this.metadata = null;
            this.fieldNames = in.readStringList();
            int rowCount = in.readVInt();
            this.rows = new ArrayList<>(rowCount);
            for (int r = 0; r < rowCount; r++) {
                int colCount = in.readVInt();
                Object[] row = new Object[colCount];
                for (int c = 0; c < colCount; c++) {
                    row[c] = in.readGenericValue();
                }
                rows.add(row);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (metadata != null) {
            out.writeBoolean(true);
            out.writeMap(metadata, StreamOutput::writeString, StreamOutput::writeString);
        } else {
            out.writeBoolean(false);
            out.writeStringCollection(fieldNames);
            out.writeVInt(rows.size());
            for (Object[] row : rows) {
                out.writeVInt(row.length);
                for (Object cell : row) {
                    out.writeGenericValue(cell);
                }
            }
        }
    }

    public boolean hasMetadata() {
        return metadata != null;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<Object[]> getRows() {
        return rows;
    }
}
