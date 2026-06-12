/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * JSON codec for the whole-plan FFM contract (whole-plan-lowering-spec.md D12) — the Java side of
 * the Rust {@code whole_plan::QueryPlanInput} / {@code QueryPlanOutput} serde types.
 *
 * <p>Metadata is JSON (not protobuf) by design: it is in-process, same-deployment, and low-volume.
 * Byte payloads (Substrait, finalized plan, Arrow IPC schema) ride as base64 strings. Field names
 * match the serde structs field-for-field.
 *
 * @opensearch.internal
 */
public final class QueryPlanJson {

    private static final Base64.Encoder B64_ENC = Base64.getEncoder();
    private static final Base64.Decoder B64_DEC = Base64.getDecoder();

    private QueryPlanJson() {}

    /** One scan's metadata (mirrors Rust {@code ScanJson}); {@code delegated} stays empty until Phase 3. */
    public record ScanInput(String table, int treeShape, boolean requestsRowIds) {}

    /** One finalized stage (mirrors Rust {@code StageJson}). */
    public record StageOutput(int boundaryId, int[] childBoundaryIds, byte[] planBytes, byte[] outputSchemaIpc) {}

    /**
     * Encode a {@code QueryPlanInput} JSON document.
     *
     * @param queryId       opaque query id (diagnostics)
     * @param substraitBytes the stitched whole-query Substrait plan bytes
     * @param scans         per-scan metadata
     */
    public static byte[] encodeInput(String queryId, byte[] substraitBytes, List<ScanInput> scans) {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            b.field("query_id", queryId);
            b.field("substrait_b64", B64_ENC.encodeToString(substraitBytes));
            b.startArray("scans");
            for (ScanInput s : scans) {
                b.startObject();
                b.field("table", s.table());
                b.field("tree_shape", s.treeShape());
                b.field("requests_row_ids", s.requestsRowIds());
                b.startArray("delegated").endArray(); // Phase 3
                b.endObject();
            }
            b.endArray();
            b.endObject();
            return org.opensearch.core.common.bytes.BytesReference.toBytes(org.opensearch.core.common.bytes.BytesReference.bytes(b));
        } catch (IOException e) {
            throw new UncheckedIOException("QueryPlanJson.encodeInput", e);
        }
    }

    /** Parse a {@code QueryPlanOutput} JSON document into per-stage outputs. */
    public static List<StageOutput> decodeOutput(byte[] json) {
        List<StageOutput> stages = new ArrayList<>();
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, json)
        ) {
            ensure(parser.nextToken() == XContentParser.Token.START_OBJECT, "expected object");
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String field = parser.currentName();
                parser.nextToken();
                if ("stages".equals(field)) {
                    ensure(parser.currentToken() == XContentParser.Token.START_ARRAY, "stages must be an array");
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        stages.add(parseStage(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("QueryPlanJson.decodeOutput", e);
        }
        return stages;
    }

    private static StageOutput parseStage(XContentParser parser) throws IOException {
        int boundaryId = 0;
        int[] childIds = new int[0];
        byte[] planBytes = new byte[0];
        byte[] schemaIpc = new byte[0];
        ensure(parser.currentToken() == XContentParser.Token.START_OBJECT, "stage must be an object");
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String field = parser.currentName();
            parser.nextToken();
            switch (field) {
                case "boundary_id" -> boundaryId = parser.intValue();
                case "child_boundary_ids" -> {
                    List<Integer> ids = new ArrayList<>();
                    ensure(parser.currentToken() == XContentParser.Token.START_ARRAY, "child_boundary_ids must be an array");
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        ids.add(parser.intValue());
                    }
                    childIds = ids.stream().mapToInt(Integer::intValue).toArray();
                }
                case "plan_bytes_b64" -> planBytes = B64_DEC.decode(parser.text());
                case "output_schema_ipc_b64" -> schemaIpc = B64_DEC.decode(parser.text());
                default -> parser.skipChildren();
            }
        }
        return new StageOutput(boundaryId, childIds, planBytes, schemaIpc);
    }

    private static void ensure(boolean cond, String message) {
        if (!cond) {
            throw new IllegalStateException("QueryPlanJson: " + message);
        }
    }
}
