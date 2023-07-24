/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.index.IndexFileNames;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * This class is a version of Lucene's ReplicaFileDeleter class used to keep track of
 * segment files that should be preserved on replicas between replication events.
 *
 * https://github.com/apache/lucene/blob/main/lucene/replicator/src/java/org/apache/lucene/replicator/nrt/ReplicaFileDeleter.java
 *
 * @opensearch.internal
 */
final class ReplicaFileTracker {

    private final Map<String, Integer> refCounts = new HashMap<>();
    private final BiConsumer<String, String> fileDeleter;

    public ReplicaFileTracker(BiConsumer<String, String> fileDeleter) {
        this.fileDeleter = fileDeleter;
    }

    public synchronized void incRef(Collection<String> fileNames) {
        for (String fileName : fileNames) {
            refCounts.merge(fileName, 1, Integer::sum);
        }
    }

    public synchronized void decRef(Collection<String> fileNames) {
        Set<String> toDelete = new HashSet<>();
        for (String fileName : fileNames) {
            Integer curCount = refCounts.get(fileName);
            // assert curCount != null : "fileName=" + fileName;
            // assert curCount > 0;
            if (curCount == 1) {
                refCounts.remove(fileName);
                toDelete.add(fileName);
            } else {
                refCounts.put(fileName, curCount - 1);
            }
        }
        delete(toDelete);
    }

    private synchronized void delete(Collection<String> toDelete) {
        // First pass: delete any segments_N files. We do these first to be certain stale commit points
        // are removed
        // before we remove any files they reference, in case we crash right now:
        for (String fileName : toDelete) {
            if (fileName.startsWith(IndexFileNames.SEGMENTS)) {
                delete(fileName);
            }
        }
        for (String fileName : toDelete) {
            if (fileName.startsWith(IndexFileNames.SEGMENTS) == false) {
                delete(fileName);
            }
        }
    }

    private synchronized void delete(String fileName) {
        assert canDelete(fileName);
        fileDeleter.accept("delete unreferenced", fileName);
    }

    public synchronized boolean canDelete(String fileName) {
        return refCounts.containsKey(fileName) == false;
    }
}
