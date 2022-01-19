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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.search.ReferenceManager;

import org.apache.lucene.search.SearcherManager;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;

/**
 * Utility class to safely share {@link OpenSearchDirectoryReader} instances across
 * multiple threads, while periodically reopening. This class ensures each
 * reader is closed only once all threads have finished using it.
 *
 * @see SearcherManager
 *
 */
@SuppressForbidden(reason = "reference counting is required here")
class OpenSearchReaderManager extends ReferenceManager<OpenSearchDirectoryReader> {
    protected static Logger logger = LogManager.getLogger(OpenSearchReaderManager.class);

    private final BiConsumer<OpenSearchDirectoryReader, OpenSearchDirectoryReader> refreshListener;

    private volatile SegmentInfos currentInfos;

    /**
     * Creates and returns a new OpenSearchReaderManager from the given
     * already-opened {@link OpenSearchDirectoryReader}, stealing
     * the incoming reference.
     *
     * @param reader            the directoryReader to use for future reopens
     * @param refreshListener   A consumer that is called every time a new reader is opened
     */
    OpenSearchReaderManager(
        OpenSearchDirectoryReader reader,
        BiConsumer<OpenSearchDirectoryReader, OpenSearchDirectoryReader> refreshListener
    ) {
        this.current = reader;
        this.refreshListener = refreshListener;
        refreshListener.accept(current, null);
    }

    @Override
    protected void decRef(OpenSearchDirectoryReader reference) throws IOException {
        reference.decRef();
    }

    @Override
    protected OpenSearchDirectoryReader refreshIfNeeded(OpenSearchDirectoryReader referenceToRefresh) throws IOException {
        // This is directly from Lucene's SegmentInfosSearcherManager...
        List<LeafReader> subs;
        if (referenceToRefresh == null) {
            subs = null;
        } else {
            subs = new ArrayList<>();
            for (LeafReaderContext ctx : referenceToRefresh.getDelegate().leaves()) {
                subs.add(ctx.reader());
            }
        }

        final OpenSearchDirectoryReader reader;
        // If not using NRT repl.
        if (currentInfos == null) {
          reader = (OpenSearchDirectoryReader) DirectoryReader.openIfChanged(referenceToRefresh);
        } else {
            // Open a new reader, sharing any common segment readers with the old one:
            DirectoryReader innerReader = StandardDirectoryReader.open(referenceToRefresh.directory(), currentInfos, subs, null);
            reader = OpenSearchDirectoryReader.wrap(innerReader, referenceToRefresh.shardId());
            logger.info("updated to SegmentInfosVersion=" + currentInfos.getVersion() + " reader=" + innerReader);
        }
        if (reader != null) {
            refreshListener.accept(reader, referenceToRefresh);
        }
        return reader;
    }

    /**
     * Switch to new segments, refreshing if necessary. Note that it's the caller job to ensure
     * there's a held refCount for the incoming infos, so all files exist.
     */
    public void setCurrentInfos(SegmentInfos infos) throws IOException {
        if (currentInfos != null) {
            // So that if we commit, we will go to the next
            // (unwritten so far) generation:
            infos.updateGeneration(currentInfos);
            logger.info("mgr.setCurrentInfos: carry over infos gen=" + infos.getSegmentsFileName());
        }
        currentInfos = infos;
        // We don't refresh here bc refresh calls are initiated on ExternalReaderManager first inside of IndexShard.
    }

    @Override
    protected boolean tryIncRef(OpenSearchDirectoryReader reference) {
        return reference.tryIncRef();
    }

    @Override
    protected int getRefCount(OpenSearchDirectoryReader reference) {
        return reference.getRefCount();
    }
}
