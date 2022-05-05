/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.Node;
import org.apache.lucene.store.AlreadyClosedException;

/**
 * Runs CopyJob(s) in background thread; each ReplicaNode has an instance of this running. At a
 * given there could be one NRT copy job running, and multiple pre-warm merged segments jobs.
 */
class Jobs extends Thread implements Closeable {

    private final PriorityQueue<CopyJob> queue = new PriorityQueue<>();

    private final Node node;

    public Jobs(Node node) {
        this.node = node;
    }

    private boolean finish;

    /**
     * Returns null if we are closing, else, returns the top job or waits for one to arrive if the
     * queue is empty.
     */
    private synchronized TransportCopyJob getNextJob() {
        while (true) {
            if (finish) {
                return null;
            } else if (queue.isEmpty()) {
                try {
                    wait();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            } else {
                return (TransportCopyJob) queue.poll();
            }
        }
    }

    @Override
    public void run() {
        while (true) {
            TransportCopyJob topJob = getNextJob();
            if (topJob == null) {
                assert finish;
                break;
            }

            this.setName("jobs o" + topJob.ord);

            assert topJob != null;

            boolean result;
            try {
                result = topJob.visit();
            } catch (Throwable t) {
                if ((t instanceof AlreadyClosedException) == false) {
                    node.message("exception during job.visit job=" + topJob + "; now cancel");
                    t.printStackTrace(System.out);
                } else {
                    node.message("AlreadyClosedException during job.visit job=" + topJob + "; now cancel");
                }
                try {
                    topJob.cancel("unexpected exception in visit", t);
                } catch (Throwable t2) {
                    node.message("ignore exception calling cancel: " + t2);
                    t2.printStackTrace(System.out);
                }
                try {
                    topJob.onceDone.run(topJob);
                } catch (Throwable t2) {
                    node.message("ignore exception calling OnceDone: " + t2);
                    t2.printStackTrace(System.out);
                }
                continue;
            }

            if (result == false) {
                // Job isn't done yet; put it back:
                synchronized (this) {
                    queue.offer(topJob);
                }
            } else {
                // Job finished, now notify caller:
                try {
                    topJob.onceDone.run(topJob);
                } catch (Throwable t) {
                    node.message("ignore exception calling OnceDone: " + t);
                    t.printStackTrace(System.out);
                }
            }
        }

        node.message("top: jobs now exit run thread");

        synchronized (this) {
            // Gracefully cancel any jobs we didn't finish:
            while (queue.isEmpty() == false) {
                TransportCopyJob job = (TransportCopyJob) queue.poll();
                node.message("top: Jobs: now cancel job=" + job);
                try {
                    job.cancel("jobs closing", null);
                } catch (Throwable t) {
                    node.message("ignore exception calling cancel");
                    t.printStackTrace(System.out);
                }
                try {
                    job.onceDone.run(job);
                } catch (Throwable t) {
                    node.message("ignore exception calling OnceDone");
                    t.printStackTrace(System.out);
                }
            }
        }
    }

    public synchronized void launch(CopyJob job) {
        if (finish == false) {
            queue.offer(job);
            notify();
        } else {
            throw new AlreadyClosedException("closed");
        }
    }

    /** Cancels any existing jobs that are copying the same file names as this one */
    public synchronized void cancelConflictingJobs(CopyJob newJob) throws IOException {
        for (CopyJob job : queue) {
            if (job.conflicts(newJob)) {
                node.message(
                    "top: now cancel existing conflicting job=" + job + " due to newJob=" + newJob);
                job.cancel("conflicts with new job", null);
            }
        }
    }

    @Override
    public synchronized void close() {
        finish = true;
        notify();
        try {
            join();
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }
}
