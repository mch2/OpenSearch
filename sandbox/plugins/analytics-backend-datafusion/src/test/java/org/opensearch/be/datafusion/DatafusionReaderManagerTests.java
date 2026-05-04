/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link DatafusionReaderManager}.
 *
 * <p>Covers the reader-lifecycle gap that surfaces in the big5 integration test: the
 * first PPL query against a freshly-created shard arrives before {@code afterRefresh}
 * has fired for the snapshot the planner picked. Previously {@code getReader} threw
 * {@code IOException("No DataFusion reader available")}; now it must lazily build on
 * miss so a valid snapshot never produces a gratuitous I/O error.
 */
public class DatafusionReaderManagerTests extends OpenSearchTestCase {

    /** Counts buildReader() invocations so tests can assert caching behavior. */
    private static final class CountingManager extends DatafusionReaderManager {
        final AtomicInteger buildCount = new AtomicInteger();

        CountingManager(DataFormat dataFormat, ShardPath shardPath, DataFusionService svc) {
            super(dataFormat, shardPath, svc);
        }

        @Override
        DatafusionReader buildReader(CatalogSnapshot catalogSnapshot) {
            buildCount.incrementAndGet();
            // No JNI call. Production DatafusionReader opens a native handle, which
            // we can't do in an OpenSearchTestCase without the Rust library on the
            // classpath. Null is acceptable because getReader uses containsKey to
            // distinguish "cached" from "never built".
            return null;
        }
    }

    private ShardPath newShardPath() throws IOException {
        ShardId shardId = new ShardId(new Index("test-index", "uuid"), 0);
        Path tmp = createTempDir().resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        java.nio.file.Files.createDirectories(tmp);
        return new ShardPath(false, tmp, tmp, shardId);
    }

    private DataFormat newParquetDataFormat() {
        return new DataFormat() {
            @Override
            public String name() {
                return "parquet";
            }

            @Override
            public long priority() {
                return 0;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of();
            }
        };
    }

    /** Minimal CatalogSnapshot that returns an empty searchable-files set. */
    private static CatalogSnapshot emptySnapshot(long generation) {
        return new CatalogSnapshot("test", generation, generation) {
            @Override
            protected void closeInternal() {}

            @Override
            public Map<String, String> getUserData() {
                return Map.of();
            }

            @Override
            public long getId() {
                return generation;
            }

            @Override
            public List<Segment> getSegments() {
                return List.of();
            }

            @Override
            public Collection<WriterFileSet> getSearchableFiles(String dataFormat) {
                return Collections.emptyList();
            }

            @Override
            public Set<String> getDataFormats() {
                return Set.of("parquet");
            }

            @Override
            public long getLastWriterGeneration() {
                return generation;
            }

            @Override
            public String serializeToString() throws IOException {
                return "";
            }

            @Override
            public void setUserData(Map<String, String> userData, boolean commitData) {}

            @Override
            public CatalogSnapshot clone() {
                return this;
            }

            @Override
            public int getFormatVersionForFile(String file) {
                return 0;
            }

            @Override
            public byte[] serialize() throws IOException {
                return new byte[0];
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
            }

            @Override
            public Collection<String> getFiles(boolean includeSegmentsFile) throws IOException {
                return Collections.emptyList();
            }
        };
    }

    /** getReader must lazily build + cache a reader when afterRefresh hasn't fired
     *  yet for the requested snapshot, instead of throwing "No DataFusion reader
     *  available". This closes the gap exposed by testSpanByImplicitTimestamp
     *  where the first query arrives before the background refresh hook. */
    public void testGetReaderLazilyBuildsOnMiss() throws IOException {
        CountingManager mgr = new CountingManager(newParquetDataFormat(), newShardPath(), null);
        CatalogSnapshot snapshot = emptySnapshot(1L);

        // Must not throw "No DataFusion reader available".
        mgr.getReader(snapshot);

        assertEquals("buildReader must be called exactly once on a cold miss", 1, mgr.buildCount.get());
    }

    /** Second call for the same snapshot must return the cached reader — no rebuild. */
    public void testGetReaderCachesLazilyBuiltReader() throws IOException {
        CountingManager mgr = new CountingManager(newParquetDataFormat(), newShardPath(), null);
        CatalogSnapshot snapshot = emptySnapshot(1L);

        mgr.getReader(snapshot);
        mgr.getReader(snapshot);

        assertEquals("buildReader must be invoked once across two getReader calls", 1, mgr.buildCount.get());
    }

    /** Distinct snapshots map to distinct cache entries. */
    public void testGetReaderBuildsPerSnapshot() throws IOException {
        CountingManager mgr = new CountingManager(newParquetDataFormat(), newShardPath(), null);
        CatalogSnapshot snap1 = emptySnapshot(1L);
        CatalogSnapshot snap2 = emptySnapshot(2L);

        mgr.getReader(snap1);
        mgr.getReader(snap2);

        assertEquals("one buildReader call per distinct snapshot", 2, mgr.buildCount.get());
    }

    /** If afterRefresh already populated the cache, getReader must not rebuild. */
    public void testGetReaderHitsCacheFromAfterRefresh() throws IOException {
        CountingManager mgr = new CountingManager(newParquetDataFormat(), newShardPath(), null);
        CatalogSnapshot snapshot = emptySnapshot(1L);

        mgr.afterRefresh(true, snapshot);
        assertEquals("afterRefresh must invoke buildReader once", 1, mgr.buildCount.get());

        mgr.getReader(snapshot);
        assertEquals("getReader must not rebuild when afterRefresh already cached", 1, mgr.buildCount.get());
    }
}
