/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;

public class DummyCodec extends FilterCodec {

    public static final String CODEC_NAME = "DummyCodec";

    public DummyCodec() {
        super(CODEC_NAME, new Lucene99Codec());
    }
}
