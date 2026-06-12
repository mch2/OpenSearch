/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag.proto;

import java.util.Arrays;

/**
 * Minimal protobuf3 wire-format reader (df-proto migration §5) — the decode
 * counterpart to {@link ProtoWriter}. Used to read the native finalizer's
 * {@code FinalizeResponse}. Supports varint (wire type 0), length-delimited
 * (wire type 2), and skipping the fixed-width wire types (1, 5) for forward
 * compatibility.
 *
 * @opensearch.internal
 */
public final class ProtoReader {

    private final byte[] buf;
    private int pos;

    public ProtoReader(byte[] buf) {
        this.buf = buf;
        this.pos = 0;
    }

    public boolean hasRemaining() {
        return pos < buf.length;
    }

    public long readVarint() {
        long result = 0;
        int shift = 0;
        while (true) {
            if (pos >= buf.length) {
                throw new IllegalStateException("protobuf varint truncated at offset " + pos);
            }
            byte b = buf[pos++];
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift >= 64) {
                throw new IllegalStateException("protobuf varint too long");
            }
        }
    }

    public byte[] readLengthDelimited() {
        int len = (int) readVarint();
        if (pos + len > buf.length) {
            throw new IllegalStateException("protobuf length-delimited field overruns buffer");
        }
        byte[] out = Arrays.copyOfRange(buf, pos, pos + len);
        pos += len;
        return out;
    }

    /** Skip an unknown field by wire type (forward-compatibility). */
    public void skip(int wireType) {
        switch (wireType) {
            case 0 -> readVarint();
            case 2 -> {
                int len = (int) readVarint();
                pos += len;
            }
            case 5 -> pos += 4; // 32-bit
            case 1 -> pos += 8; // 64-bit
            default -> throw new IllegalStateException("unsupported protobuf wire type " + wireType);
        }
    }
}
