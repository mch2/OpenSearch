/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class NRTIndexCommit extends IndexCommit {

    private final SegmentInfos infos;
    private final IndexWriter writer;

    public NRTIndexCommit(SegmentInfos segmentInfos, IndexWriter indexWriter) {
        this.infos = segmentInfos;
        this.writer = indexWriter;
    }

    @Override
    public String getSegmentsFileName() {
        return infos.getSegmentsFileName();
    }

    @Override
    public Collection<String> getFileNames() throws IOException {
        return Collections.unmodifiableCollection(infos.files(false));
    }

    @Override
    public Directory getDirectory() {
        return writer.getDirectory();
    }

    @Override
    public void delete() {
        try {
            writer.decRefDeleter(infos);
        } catch (IOException e) {
            throw new RuntimeException("Could not delete commit", e);
        }
    }

    @Override
    public boolean isDeleted() {
        return false;
    }

    @Override
    public int getSegmentCount() {
        return infos.size();
    }

    @Override
    public long getGeneration() {
        return infos.getGeneration();
    }

    @Override
    public Map<String, String> getUserData() throws IOException {
        return infos.getUserData();
    }

    public SegmentInfos getInfos() {
        return infos;
    }
}
