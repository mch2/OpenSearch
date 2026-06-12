/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag.proto;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Minimal protobuf3 wire-format writer (df-proto migration §5).
 *
 * <p>The Brazil/Gradle Java build does not run {@code protoc}, so the
 * coordinator-side encoder for {@code StageMeta} / {@code FinalizeRequest} is
 * hand-written against the same canonical {@code proto/stage.proto} the Rust
 * side compiles via hand-maintained prost structs. This writer implements just
 * the wire-format primitives those messages need: varint, length-delimited
 * (bytes/string/sub-message), and the proto3 default-value elision rule
 * (scalar fields equal to their type default are not written, exactly matching
 * prost's output so the round-trip is byte-compatible).
 *
 * <p>Field encoding: each field is a key (tag &lt;&lt; 3 | wireType) followed by
 * the payload. Wire types used here: 0 (varint) and 2 (length-delimited).
 *
 * @opensearch.internal
 */
public final class ProtoWriter {

    private static final int WIRE_VARINT = 0;
    private static final int WIRE_LEN = 2;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    /** int32 field (proto3: omitted when 0). */
    public ProtoWriter int32(int fieldNumber, int value) {
        if (value != 0) {
            writeKey(fieldNumber, WIRE_VARINT);
            // proto3 encodes negative int32 as a 10-byte varint (sign-extended to 64 bits).
            writeVarint(value & 0xFFFFFFFFL | (value < 0 ? 0xFFFFFFFF_00000000L : 0L));
        }
        return this;
    }

    /** Repeated int32 (packed, as prost emits for `repeated int32`). */
    public ProtoWriter packedInt32(int fieldNumber, int[] values) {
        if (values != null && values.length > 0) {
            ByteArrayOutputStream packed = new ByteArrayOutputStream();
            for (int v : values) {
                writeVarintTo(packed, v & 0xFFFFFFFFL | (v < 0 ? 0xFFFFFFFF_00000000L : 0L));
            }
            byte[] body = packed.toByteArray();
            writeKey(fieldNumber, WIRE_LEN);
            writeVarint(body.length);
            out.writeBytes(body);
        }
        return this;
    }

    /** bool field (proto3: omitted when false). */
    public ProtoWriter bool(int fieldNumber, boolean value) {
        if (value) {
            writeKey(fieldNumber, WIRE_VARINT);
            writeVarint(1);
        }
        return this;
    }

    /** enum field — encoded as a varint int32 (proto3: omitted when 0). */
    public ProtoWriter enumValue(int fieldNumber, int ordinal) {
        return int32(fieldNumber, ordinal);
    }

    /** string field (proto3: omitted when empty). */
    public ProtoWriter string(int fieldNumber, String value) {
        if (value != null && !value.isEmpty()) {
            bytes(fieldNumber, value.getBytes(StandardCharsets.UTF_8));
        }
        return this;
    }

    /** bytes field (proto3: omitted when empty). */
    public ProtoWriter bytes(int fieldNumber, byte[] value) {
        if (value != null && value.length > 0) {
            writeKey(fieldNumber, WIRE_LEN);
            writeVarint(value.length);
            out.writeBytes(value);
        }
        return this;
    }

    /**
     * Repeated-bytes element that is ALWAYS emitted, even when empty. Proto3 elides empty
     * scalar bytes, but a {@code repeated bytes} used for positional alignment (parallel to
     * another repeated field) must emit every element so the decoder's index matches. Each
     * call writes one length-delimited occurrence of {@code fieldNumber}.
     */
    public ProtoWriter bytesAllowEmpty(int fieldNumber, byte[] value) {
        byte[] v = value != null ? value : new byte[0];
        writeKey(fieldNumber, WIRE_LEN);
        writeVarint(v.length);
        out.writeBytes(v);
        return this;
    }

    /** Embedded sub-message (always written when non-null, even if empty — matches `optional`/repeated semantics). */
    public ProtoWriter message(int fieldNumber, byte[] encoded) {
        if (encoded != null) {
            writeKey(fieldNumber, WIRE_LEN);
            writeVarint(encoded.length);
            out.writeBytes(encoded);
        }
        return this;
    }

    public byte[] toByteArray() {
        return out.toByteArray();
    }

    private void writeKey(int fieldNumber, int wireType) {
        writeVarint(((long) fieldNumber << 3) | wireType);
    }

    private void writeVarint(long value) {
        writeVarintTo(out, value);
    }

    private static void writeVarintTo(ByteArrayOutputStream sink, long value) {
        while (true) {
            if ((value & ~0x7FL) == 0) {
                sink.write((int) value);
                return;
            }
            sink.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
    }
}
