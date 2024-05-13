/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;

public class DummyCodec extends FilterCodec {

    public static final Logger logger = LogManager.getLogger(DummyCodec.class);
    public static final String CODEC_NAME = "Lucene99";

    public DummyCodec() {
        super(CODEC_NAME, new Lucene99Codec());
        logger.info("Loading my codec");
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        // return custom format
        return super.storedFieldsFormat();
    }
}
