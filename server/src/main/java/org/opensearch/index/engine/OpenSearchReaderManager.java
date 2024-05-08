/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.index.codec.DummyCodec;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.index.engine.NRTReplicationReaderManager.unwrapStandardReader;

/**
 * Utility class to safely share {@link OpenSearchDirectoryReader} instances across
 * multiple threads, while periodically reopening. This class ensures each
 * reader is closed only once all threads have finished using it.
 *
 * @opensearch.internal
 * @see SearcherManager
 */
@SuppressForbidden(reason = "reference counting is required here")
class OpenSearchReaderManager extends ReferenceManager<OpenSearchDirectoryReader> {

    public static final Logger logger = LogManager.getLogger(OpenSearchReaderManager.class);

    private volatile OpenSearchDirectoryReader innerReader;

    /**
     * Creates and returns a new OpenSearchReaderManager from the given
     * already-opened {@link OpenSearchDirectoryReader}, stealing
     * the incoming reference.
     *
     * @param reader the directoryReader to use for future reopens
     */
    OpenSearchReaderManager(OpenSearchDirectoryReader reader) {
        this.current = reader;
        this.innerReader = current;
    }

    @Override
    protected void decRef(OpenSearchDirectoryReader reference) throws IOException {
        reference.decRef();
    }

    @Override
    protected OpenSearchDirectoryReader refreshIfNeeded(OpenSearchDirectoryReader referenceToRefresh) throws IOException {
        // call refresh our internal reference - built from IW
        final OpenSearchDirectoryReader reader = (OpenSearchDirectoryReader) DirectoryReader.openIfChanged(innerReader);
        if (reader != null) {
            OpenSearchDirectoryReader prev = this.innerReader;
            this.innerReader = reader;
            try {
                // iff there are changes build a new reader to return
                StandardDirectoryReader sdr = unwrapStandardReader(reader);
                SegmentInfos sis = sdr.getSegmentInfos().clone();
                replace(sis);
                // maaaybe we can reuse parts of readers here - need to dig.
                return OpenSearchDirectoryReader.wrap(StandardDirectoryReader.open(referenceToRefresh.directory(), sis, null, null),
                    referenceToRefresh.shardId());
            } finally {
                // release the previous innerReader
                if (referenceToRefresh != prev) {
                    release(prev);
                }
            }
        }
        return null;
    }

    private void replace(SegmentInfos sis) {
        List<SegmentCommitInfo> segmentCommitInfos = sis.asList().stream().map(sci -> {
            logger.info("sci.info.codec {}", sci.info.getCodec().toString());
            return updateSegmentInfo(sci);
        }).collect(Collectors.toList());
        sis.clear();
        sis.addAll(segmentCommitInfos);
    }

    private SegmentCommitInfo updateSegmentInfo(SegmentCommitInfo oldSci) {
        SegmentInfo oldSi = oldSci.info;
        SegmentInfo newSi =
            new SegmentInfo(oldSi.dir, oldSi.getVersion(), oldSi.getMinVersion(),
                oldSi.name, oldSi.maxDoc(), oldSi.getUseCompoundFile(), oldSi.getHasBlocks(),
                Codec.forName(DummyCodec.CODEC_NAME), // do the replace
                oldSi.getDiagnostics(), oldSi.getId(), oldSi.getAttributes(),
                oldSi.getIndexSort());
        newSi.setFiles(oldSi.files());
        SegmentCommitInfo newSci =
            new SegmentCommitInfo(newSi, oldSci.getDelCount(), oldSci.getSoftDelCount(),
                oldSci.getDelGen(), oldSci.getFieldInfosGen(), oldSci.getDocValuesGen(), oldSci.getId());
        newSci.setFieldInfosFiles(oldSci.getFieldInfosFiles());
        newSci.setDocValuesUpdatesFiles(oldSci.getDocValuesUpdatesFiles());
        return newSci;
    }

    @Override
    protected boolean tryIncRef(OpenSearchDirectoryReader reference) {
        return reference.tryIncRef();
    }

    @Override
    protected int getRefCount(OpenSearchDirectoryReader reference) {
        return reference.getRefCount();
    }

    @Override
    protected void afterClose() throws IOException {
        release(this.innerReader);
    }
}
