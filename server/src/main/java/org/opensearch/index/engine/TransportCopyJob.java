/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.CopyOneFile;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.Node;
import org.apache.lucene.replicator.nrt.NodeCommunicationException;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.apache.lucene.util.IOUtils;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.indices.replication.copy.PrimaryShardReplicationSource;
import org.opensearch.indices.replication.copy.SegmentReplicationPrimaryService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class TransportCopyJob extends CopyJob {
    private static final Logger logger = LogManager.getLogger(TransportCopyJob.class);

    private Store store;
    private CopyState copyState;
    private final PrimaryShardReplicationSource replicationSource;
    private Iterator<Map.Entry<String, FileMetaData>> iter;
    ShardId shardId;

    protected TransportCopyJob(String reason,
                               ShardId shardId,
                               Store store,
                               PrimaryShardReplicationSource replicationSource,
                               CopyState copyState,
                               ReplicaNode dest,
                               boolean highPriority,
                               OnceDone onceDone) throws IOException {
        super(reason, copyState.files, dest, highPriority, onceDone);
        this.shardId = shardId;
        this.copyState = copyState;
        this.store = store;
        this.replicationSource = replicationSource;
    }

    @Override
    protected CopyOneFile newCopyOneFile(CopyOneFile current) {
        return current;
    }

    @Override
    public void start() throws IOException {
        if (iter == null) {
            iter = toCopy.iterator();
            // This means we resumed an already in-progress copy; we do this one first:
            if (current != null) {
                totBytes += current.metaData.length;
            }
            for (Map.Entry<String, FileMetaData> ent : toCopy) {
                FileMetaData metaData = ent.getValue();
                totBytes += metaData.length;
            }

            // Send all file names / offsets up front to avoid ping-ping latency:
            try {
                dest.message(
                    "SimpleCopyJob.init: done start files count="
                        + toCopy.size()
                        + " totBytes="
                        + totBytes);
            } catch (Throwable t) {
                cancel("exc during start", t);
                throw new NodeCommunicationException("exc during start", t);
            }
        } else {
            throw new IllegalStateException("already started");
        }
    }

    @Override
    public void runBlocking() throws Exception {
        while (visit() == false) ;
        if (getFailed()) {
            throw new RuntimeException("copy failed: " + cancelReason, exc);
        }
    }

    @Override
    public boolean conflicts(CopyJob _other) {
        Set<String> filesToCopy = new HashSet<>();
        for (Map.Entry<String, FileMetaData> ent : toCopy) {
            filesToCopy.add(ent.getKey());
        }

        TransportCopyJob other = (TransportCopyJob) _other;
        synchronized (other) {
            for (Map.Entry<String, FileMetaData> ent : other.toCopy) {
                if (filesToCopy.contains(ent.getKey())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void finish() throws IOException {
        store.renameTempFilesSafe(copiedFiles);
    }

    @Override
    public boolean getFailed() {
        final Throwable exc = this.exc;
        logger.error("error", exc);
        return exc != null;
    }

    @Override
    public Set<String> getFileNamesToCopy() {
        Set<String> fileNames = new HashSet<>();
        for (Map.Entry<String, FileMetaData> ent : toCopy) {
            fileNames.add(ent.getKey());
        }
        return fileNames;
    }

    @Override
    public Set<String> getFileNames() {
        return files.keySet();
    }

    @Override
    public CopyState getCopyState() {
        return this.copyState;
    }

    @Override
    public long getTotalBytesCopied() {
        return totBytesCopied;
    }

    @Override
    public int compareTo(CopyJob _other) {
        TransportCopyJob other = (TransportCopyJob) _other;
        if (highPriority != other.highPriority) {
            return highPriority ? -1 : 1;
        } else if (ord < other.ord) {
            return -1;
        } else if (ord > other.ord) {
            return 1;
        } else {
            return 0;
        }
    }

    /** Do an iota of work; returns true if all copying is done */
    public synchronized boolean visit() throws IOException {
        if (exc != null) {
            // We were externally cancelled:
            return true;
        }
        if (current == null) {
            if (iter.hasNext() == false) {
                return true;
            }
            Map.Entry<String, FileMetaData> next = iter.next();
            FileMetaData metaData = next.getValue();
            String fileName = next.getKey();

            current = new CopyOneFile(shardId, dest, fileName, metaData, replicationSource);
        }
        if (current.visit()) {
            // This file is done copying
            copiedFiles.put(current.name, current.tmpName);
            totBytesCopied += current.bytesCopied;
            assert totBytesCopied <= totBytes
                : "totBytesCopied=" + totBytesCopied + " totBytes=" + totBytes;
            current = null;
            return false;
        }
        return false;
    }

    @Override
    public void cancel(String reason, Throwable exc) throws IOException {
        if (this.exc != null) {
            // Already cancelled
            return;
        }

        dest.message(
            String.format(
                Locale.ROOT,
                "top: cancel after copying %s; exc=%s:\n  files=%s\n  copiedFiles=%s",
                Node.bytesToString(totBytesCopied),
                exc,
                files == null ? "null" : files.keySet(),
                copiedFiles.keySet()));

        if (exc == null) {
            exc = new Throwable();
        }

        this.exc = exc;
        this.cancelReason = reason;

        // Delete all temp files we wrote:
        IOUtils.deleteFilesIgnoringExceptions(dest.getDirectory(), copiedFiles.values());

        if (current != null) {
            IOUtils.closeWhileHandlingException(current);
            if (Node.VERBOSE_FILES) {
                dest.message("remove partial file " + current.tmpName);
            }
//            dest.deleter.deleteNewFile(current.tmpName);
            current = null;
        }
    }
}
