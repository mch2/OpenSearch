/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.substrait.relation.Extension;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.Type;

/**
 * The isthmus POJO detail for an {@code os_stage_boundary} extension-single relation
 * (whole-plan-lowering-spec.md D2). A stage boundary is a single-input passthrough, so its record
 * type is its input's. Serializes to a {@link com.google.protobuf.Any} with
 * {@code type_url == "os_stage_boundary"} and JSON value {@code {"boundary_id":N,"exchange_type":"GATHER"}}
 * — the exact bytes the Rust {@code StageBoundarySerializerRegistry} consumes.
 *
 * <p>Stitching at the POJO level (rather than the raw proto) lets isthmus's
 * {@code PlanProtoConverter} re-collect every stage's function/type extensions into ONE consistent
 * anchor table at serialization — proto-level splicing would dangle each stage's independently
 * numbered {@code function_reference} anchors.
 *
 * @opensearch.internal
 */
public final class StageBoundaryDetail implements Extension.SingleRelDetail {

    /** Substrait extension-relation type URL — must match Rust's {@code STAGE_BOUNDARY_TYPE_URL}. */
    static final String TYPE_URL = "os_stage_boundary";

    private final int boundaryId;

    StageBoundaryDetail(int boundaryId) {
        this.boundaryId = boundaryId;
    }

    @Override
    public Any toProto(RelProtoConverter converter) {
        // JSON must match Rust's StageBoundaryDetail serde: {boundary_id, exchange_type:"GATHER"}.
        String json = "{\"boundary_id\":" + boundaryId + ",\"exchange_type\":\"GATHER\"}";
        return Any.newBuilder().setTypeUrl(TYPE_URL).setValue(ByteString.copyFromUtf8(json)).build();
    }

    @Override
    public Type.Struct deriveRecordType(Rel input) {
        // A boundary is a passthrough — its output schema is its input's.
        return input.getRecordType();
    }
}
