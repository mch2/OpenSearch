/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Integration test validating dynamic mapping support with composite parquet index.
 * Documents with new fields (not in the original mapping) should be indexed successfully,
 * and the resulting Parquet files should contain all fields including dynamically added ones.
 * <p>
 * Requires JDK 25 and sandbox enabled. Run with:
 * ./gradlew :sandbox:plugins:composite-engine:internalClusterTest \
 * --tests "*.CompositeDynamicMappingIT" \
 * -Dsandbox.enabled=true
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class CompositeDynamicMappingIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-dynamic-mapping";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    /**
     * Tests dynamic mapping with parquet primary + lucene secondary.
     * Verifies that dynamically added fields appear in both formats.
     * <p>
     * Note: The Lucene secondary writer stores inverted indexes for text/keyword fields
     * (for search) and __row_id__ as doc values (for cross-format correlation).
     * Numeric fields and field values are only in Parquet.
     */
    public void testDynamicMappingWithParquetPrimaryLuceneSecondary() throws Exception {
        String indexName = "test-dynamic-composite";

        CreateIndexResponse createResponse = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(parquetPrimaryLuceneSecondarySettings())
            .setMapping("field_keyword", "type=keyword", "field_number", "type=integer")
            .get();
        assertTrue(createResponse.isAcknowledged());
        ensureGreen(indexName);

        // Index docs with initial schema
        for (int i = 0; i < 5; i++) {
            IndexResponse response = client().prepareIndex()
                .setIndex(indexName)
                .setSource("field_keyword", "value_" + i, "field_number", i)
                .get();
            assertEquals(RestStatus.CREATED, response.status());
        }

        // Index docs with dynamic fields
        indexDocsWithDynamicFields(indexName, 5, 10);

        // Refresh + flush
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();

        // Verify parquet
        IndexShard shard = getIndexShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        try (GatedCloseable<List<Path>> parquetFilesRef = listParquetFiles(parquetDir, shard)) {
            List<Path> parquetFiles = parquetFilesRef.get();
            List<Map<String, Object>> parquetRows = readAllParquetRows(parquetFiles);
            assertEquals("Parquet should have 10 rows", 10, parquetRows.size());
            assertDynamicFieldCount(parquetRows, "dynamic_text", 5);
            assertDynamicFieldCount(parquetRows, "dynamic_long", 5);
        }

        // Verify lucene secondary: doc count + indexed fields present
        Path luceneDir = shard.shardPath().resolveIndex();
        List<Map<String, Object>> luceneRows = readAllLuceneDocs(luceneDir);
        assertEquals("Lucene should have 10 docs", 10, luceneRows.size());

        // __row_id__ should be present in all docs (only doc values field in lucene secondary)
        long rowsWithRowId = luceneRows.stream().filter(r -> r.containsKey("__row_id__")).count();
        assertEquals("All 10 Lucene docs should have __row_id__", 10, rowsWithRowId);

        // Verify that the lucene index has the expected indexed fields (inverted index)
        assertLuceneIndexedFieldsPresent(luceneDir, Set.of("field_keyword", "dynamic_text"));
        ensureNoActiveMerges(indexName);
    }

    public void testConflictingDynamicMappings() {
        String indexName = "test-conflict";

        CreateIndexResponse createResponse = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(parquetPrimaryLuceneSecondarySettings())
            .get();
        assertTrue(createResponse.isAcknowledged());
        ensureGreen(indexName);

        // First doc: foo inferred as long
        client().prepareIndex(indexName).setSource("foo", 3).get();

        // Second doc: foo as text — should fail
        try {
            client().prepareIndex(indexName).setSource("foo", "bar").get();
            fail("Indexing request should have failed!");
        } catch (Exception e) {
            assertTrue(
                "Expected type conflict error but got: " + e.getMessage(),
                e.getMessage().contains("failed to parse field [foo] of type [long]")
                    || e.getMessage().contains("mapper [foo] cannot be changed from type [long] to [text]")
            );
        }
    }

    /**
     * Tests concurrent dynamic mapping updates with parquet primary + lucene secondary.
     * Verifies both formats contain all dynamically created fields.
     */
    public void testConcurrentDynamicUpdatesWithLuceneSecondary() throws Throwable {
        String indexName = "test-concurrent-composite";

        CreateIndexResponse createResponse = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(parquetPrimaryLuceneSecondarySettings())
            .get();
        assertTrue(createResponse.isAcknowledged());
        ensureGreen(indexName);

        final int numThreads = 32;
        runConcurrentIndexing(indexName, numThreads);

        // Verify mappings
        assertConcurrentMappings(indexName, numThreads);

        // Verify parquet
        IndexShard shard = getIndexShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        try (GatedCloseable<List<Path>> parquetFilesRef = listParquetFiles(parquetDir, shard)) {
            List<Path> parquetFiles = parquetFilesRef.get();
            assertEquals("Parquet total rows should be 64", 64, getParquetRowCount(parquetFiles));
            List<Map<String, Object>> parquetRows = readAllParquetRows(parquetFiles);
            assertEquals(64, parquetRows.size());
            assertConcurrentFieldValues(parquetRows, numThreads);
        }

        // Verify lucene secondary: doc count + all dynamic fields indexed
        Path luceneDir = shard.shardPath().resolveIndex();
        List<Map<String, Object>> luceneRows = readAllLuceneDocs(luceneDir);
        assertEquals("Lucene doc count should be 64", 64, luceneRows.size());

        // __row_id__ present in all docs
        long rowsWithRowId = luceneRows.stream().filter(r -> r.containsKey("__row_id__")).count();
        assertEquals("All 64 Lucene docs should have __row_id__", 64, rowsWithRowId);

        // Verify all dynamic fieldA_*/fieldB_* are indexed in Lucene
        Set<String> expectedFields = new HashSet<>();
        for (int i = 0; i < numThreads; i++) {
            expectedFields.add("fieldA_" + i);
            expectedFields.add("fieldB_" + i);
        }
        assertLuceneIndexedFieldsPresent(luceneDir, expectedFields);
        ensureNoActiveMerges(indexName);
    }

    /** Verifies that the default scalar shape rejects an array without changing cluster state. */
    public void testScalarKeywordRejectsArrayWithoutUpdatingClusterState() throws Exception {
        String indexName = "test-scalar-keyword";
        CreateIndexResponse createResponse = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(parquetPrimaryLuceneSecondarySettings())
            .setMapping("tags", "type=keyword")
            .get();
        assertTrue(createResponse.isAcknowledged());
        ensureGreen(indexName);

        assertFalse(clusterStateFieldMapping(indexName, "tags").containsKey("multi_value"));
        long mappingVersion = getClusterState().metadata().index(indexName).getMappingVersion();

        IndexResponse scalarResponse = client().prepareIndex(indexName).setSource("tags", "solo").get();
        assertEquals(RestStatus.CREATED, scalarResponse.status());

        Exception error = expectThrows(
            Exception.class,
            () -> client().prepareIndex(indexName).setSource("tags", List.of("one", "two")).get()
        );
        assertThat(
            org.opensearch.ExceptionsHelper.stackTrace(error),
            org.hamcrest.Matchers.containsString("declare [multi_value: true] when creating the field mapping")
        );
        assertEquals(mappingVersion, getClusterState().metadata().index(indexName).getMappingVersion());
        assertFalse(clusterStateFieldMapping(indexName, "tags").containsKey("multi_value"));

        List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
        assertEquals(1, rows.size());
        assertEquals("solo", rows.get(0).get("tags"));
    }

    /** Verifies that explicit LIST state writes scalar input as a singleton list and arrays unchanged. */
    public void testExplicitListKeywordPersistsListShapeForEveryDocument() throws Exception {
        String indexName = "test-list-keyword";
        CreateIndexResponse createResponse = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(parquetPrimaryLuceneSecondarySettings())
            .setMapping("tags", "type=keyword,multi_value=true")
            .get();
        assertTrue(createResponse.isAcknowledged());
        ensureGreen(indexName);

        assertEquals(Boolean.TRUE, clusterStateFieldMapping(indexName, "tags").get("multi_value"));
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("tags", "solo").get().status());
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("tags", List.of("one", "two", "one")).get().status());

        List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
        assertEquals(2, rows.size());
        assertTrue(rows.stream().allMatch(row -> isListColumnPlaceholder(row.get("tags"))));
    }

    // ══════════════════════════════════════════════════════════════════════
    // Dynamic array detection
    //
    // A field's shape is fixed when the field is created, so a dynamically created field has to be
    // declared array-shaped by the document that creates it. These tests work from the outside in:
    // index a document, then read the cluster-state mapping and the physical Parquet column.
    // ══════════════════════════════════════════════════════════════════════

    /** An array on an unmapped field declares the field multi-valued and writes a LIST column. */
    public void testDetectionDeclaresArrayFieldAndWritesListColumn() throws Exception {
        String indexName = "test-detect-array";
        createParquetIndex(indexName);

        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("codes", List.of(1, 2)).get().status());

        assertEquals(
            "the document that created the field declared its shape",
            Boolean.TRUE,
            clusterStateFieldMapping(indexName, "codes").get("multi_value")
        );
        assertEquals("long", clusterStateFieldMapping(indexName, "codes").get("type"));

        List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
        assertEquals(1, rows.size());
        assertTrue("physical Parquet column must be a LIST", isListColumnPlaceholder(rows.get(0).get("codes")));
    }

    /** A scalar on an unmapped field leaves it single-valued, so the column stays primitive. */
    public void testDetectionLeavesScalarFieldSingleValued() throws Exception {
        String indexName = "test-detect-scalar";
        createParquetIndex(indexName);

        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("code", 7).get().status());

        assertFalse(clusterStateFieldMapping(indexName, "code").containsKey("multi_value"));
        List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
        assertEquals(1, rows.size());
        assertFalse(isListColumnPlaceholder(rows.get(0).get("code")));
    }

    /**
     * The producer contract: emit the array form on the first document even when it carries one value.
     * The field is array-shaped from then on, so a later multi-value document is accepted.
     */
    public void testSingleElementArrayDeclaresArrayAndAcceptsMoreLater() throws Exception {
        String indexName = "test-detect-singleton";
        createParquetIndex(indexName);

        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("codes", List.of(7)).get().status());
        assertEquals(Boolean.TRUE, clusterStateFieldMapping(indexName, "codes").get("multi_value"));

        assertEquals(
            "an array-shaped field accepts additional values without a mapping change",
            RestStatus.CREATED,
            client().prepareIndex(indexName).setSource("codes", List.of(8, 9)).get().status()
        );
        assertEquals(Boolean.TRUE, clusterStateFieldMapping(indexName, "codes").get("multi_value"));
        assertEquals(2, refreshFlushAndReadParquetRows(indexName).size());
    }

    /** A scalar value on an array-shaped field is stored as a one-element list. */
    public void testDetectedArrayFieldAcceptsScalarAsSingleton() throws Exception {
        String indexName = "test-detect-scalar-into-array";
        createParquetIndex(indexName);

        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("codes", List.of(1, 2)).get().status());
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("codes", 3).get().status());

        List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
        assertEquals(2, rows.size());
        assertTrue(rows.stream().allMatch(row -> isListColumnPlaceholder(row.get("codes"))));
    }

    /**
     * The OpenTelemetry template shape. Attribute keys are user-defined, so `attributes.*` is a
     * wildcard and the template cannot name which keys hold arrays — detection has to add array-ness
     * on top of whatever the template specifies, per key.
     */
    public void testDetectionThroughAnOtelStyleDynamicTemplate() throws Exception {
        String indexName = "test-detect-otel-template";
        String mapping = "{\"dynamic_templates\":[{\"string_attributes\":{"
            + "\"path_match\":\"attributes.*\",\"match_mapping_type\":\"string\","
            + "\"mapping\":{\"type\":\"keyword\",\"ignore_above\":256}}}]}";
        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(parquetPrimaryLuceneSecondarySettings())
                .setMapping(mapping)
                .get()
                .isAcknowledged()
        );
        ensureGreen(indexName);

        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName)
                .setSource("attributes", Map.of("tags", List.of("prod", "blue"), "host", "node-1"))
                .get()
                .status()
        );

        Map<String, Object> attributes = nestedFieldMapping(indexName, "attributes");
        assertEquals("keyword", fieldOf(attributes, "tags").get("type"));
        assertEquals("the array-valued attribute is multi-valued", Boolean.TRUE, fieldOf(attributes, "tags").get("multi_value"));
        assertFalse("the scalar attribute stays single-valued", fieldOf(attributes, "host").containsKey("multi_value"));
        assertEquals("the template's own settings still apply", 256, fieldOf(attributes, "tags").get("ignore_above"));

        List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
        assertEquals(1, rows.size());
        assertTrue(isListColumnPlaceholder(rows.get(0).get("attributes.tags")));
        assertEquals("node-1", rows.get(0).get("attributes.host"));
    }

    /** Detection covers every element type an OTel attribute can carry through the template. */
    public void testDetectionCoversTheOtelAttributeTypes() throws Exception {
        String indexName = "test-detect-types";
        createParquetIndex(indexName);

        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName)
                .setSource("counts", List.of(1, 2), "ratios", List.of(1.5, 2.5), "flags", List.of(true, false))
                .get()
                .status()
        );

        for (String field : List.of("counts", "ratios", "flags")) {
            assertEquals(field + " should be multi-valued", Boolean.TRUE, clusterStateFieldMapping(indexName, field).get("multi_value"));
        }

        List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
        assertEquals(1, rows.size());
        for (String field : List.of("counts", "ratios", "flags")) {
            assertTrue(field + " should be a LIST column", isListColumnPlaceholder(rows.get(0).get(field)));
        }
    }

    /** An empty array creates nothing: there is no value to infer an element type from. */
    public void testEmptyArrayOnAnUnmappedFieldCreatesNoField() throws Exception {
        String indexName = "test-detect-empty";
        createParquetIndex(indexName);

        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("codes", List.of()).get().status());
        assertNull(
            "an empty array carries no element type to infer, so the field is not created",
            clusterStateProperties(indexName).get("codes")
        );
    }

    /**
     * Every leaf under an array of objects is declared array-shaped, including one that happens to appear
     * once — {@code [{"bar":1},{"baz":2}]}. The enclosing array is taken as the declaration; distinguishing
     * exactly would mean buffering the array to count each leaf before any mapper exists.
     */
    public void testArrayOfObjectsDeclaresEveryLeafArrayShaped() throws Exception {
        String indexName = "test-detect-object-array-distinct";
        createParquetIndex(indexName);

        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName).setSource("events", List.of(Map.of("bar", 1), Map.of("baz", 2))).get().status()
        );

        Map<String, Object> events = nestedFieldMapping(indexName, "events");
        assertEquals(Boolean.TRUE, fieldOf(events, "bar").get("multi_value"));
        assertEquals(Boolean.TRUE, fieldOf(events, "baz").get("multi_value"));
    }

    /**
     * Shape B: {@code events} is the array and each object carries its own {@code name}. Under the
     * default {@code object} mapping OpenSearch flattens this to the same index state as shape A
     * ({@code "events": {"name": [1,2]}}) — one field holding two values, sibling pairing discarded — so
     * it needs the same LIST column. The enclosing array is what declares the shape, so it is known when
     * the field is created rather than when a repeat is observed.
     */
    public void testArrayOfObjectsRepeatingALeafBecomesMultiValued() throws Exception {
        String indexName = "test-detect-object-array-repeated";
        createParquetIndex(indexName);

        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName).setSource("events", List.of(Map.of("name", 1), Map.of("name", 2))).get().status()
        );

        Map<String, Object> events = nestedFieldMapping(indexName, "events");
        assertEquals(
            "the two objects' name values belong to one multi-valued field",
            Boolean.TRUE,
            fieldOf(events, "name").get("multi_value")
        );

        List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
        assertEquals(1, rows.size());
        assertTrue(isListColumnPlaceholder(rows.get(0).get("events.name")));
    }

    /** Shape A: the leaf itself is the array. Detected at creation, since the value is literally one. */
    public void testObjectWithAnArrayValuedLeafBecomesMultiValued() throws Exception {
        String indexName = "test-detect-object-array-valued-leaf";
        createParquetIndex(indexName);

        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName).setSource("events", Map.of("name", List.of(1, 2))).get().status()
        );

        Map<String, Object> events = nestedFieldMapping(indexName, "events");
        assertEquals(Boolean.TRUE, fieldOf(events, "name").get("multi_value"));

        List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
        assertEquals(1, rows.size());
        assertTrue(isListColumnPlaceholder(rows.get(0).get("events.name")));
    }

    /** Declaring the shape up front works too, and is what a template would do. */
    public void testArrayOfObjectsRepeatingALeafWorksWhenDeclared() throws Exception {
        String indexName = "test-declared-object-array";
        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(parquetPrimaryLuceneSecondarySettings())
                .setMapping("events.name", "type=long,multi_value=true")
                .get()
                .isAcknowledged()
        );
        ensureGreen(indexName);

        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName).setSource("events", List.of(Map.of("name", 1), Map.of("name", 2))).get().status()
        );

        List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
        assertEquals(1, rows.size());
        assertTrue(isListColumnPlaceholder(rows.get(0).get("events.name")));
    }

    /** A single object is not an array, so its leaves stay single-valued. */
    public void testSingleObjectLeavesLeafFieldsSingleValued() throws Exception {
        String indexName = "test-detect-single-object";
        createParquetIndex(indexName);

        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("event", Map.of("name", 1)).get().status());

        Map<String, Object> event = nestedFieldMapping(indexName, "event");
        assertFalse(fieldOf(event, "name").containsKey("multi_value"));
    }

    /**
     * Every type the Parquet format registers must accept a declared array and write a LIST column.
     * Failing here means a type's mapper or its {@code ParquetField} is unwired — and an unwired mapper
     * accepts {@code multi_value: true} and silently ignores it, which is worse than rejecting.
     */
    public void testDeclaredArrayWorksForEveryParquetType() throws Exception {
        Map<String, List<Object>> byType = new java.util.LinkedHashMap<>();
        byType.put("keyword", List.of("a", "b"));
        byType.put("text", List.of("alpha beta", "gamma"));
        byType.put("match_only_text", List.of("alpha beta", "gamma"));
        byType.put("long", List.of(1, 2));
        byType.put("integer", List.of(1, 2));
        byType.put("short", List.of(1, 2));
        byType.put("byte", List.of(1, 2));
        byType.put("double", List.of(1.5, 2.5));
        byType.put("float", List.of(1.5, 2.5));
        byType.put("half_float", List.of(1.5, 2.5));
        byType.put("unsigned_long", List.of(1, 2));
        byType.put("boolean", List.of(true, false));
        byType.put("date", List.of("2026-01-01T00:00:00Z", "2026-06-15T12:30:00Z"));
        byType.put("date_nanos", List.of("2026-01-01T00:00:00Z", "2026-06-15T12:30:00Z"));
        byType.put("ip", List.of("10.0.0.1", "192.168.1.1"));

        List<String> unwired = new ArrayList<>();
        for (Map.Entry<String, List<Object>> entry : byType.entrySet()) {
            String type = entry.getKey();
            String indexName = "test-alltypes-" + type.replace('_', '-');
            assertTrue(
                client().admin()
                    .indices()
                    .prepareCreate(indexName)
                    .setSettings(parquetPrimaryLuceneSecondarySettings())
                    .setMapping("f", "type=" + type + ",multi_value=true")
                    .get()
                    .isAcknowledged()
            );
            ensureGreen(indexName);

            if (Boolean.TRUE.equals(clusterStateFieldMapping(indexName, "f").get("multi_value")) == false) {
                unwired.add(type + " (mapping ignored multi_value)");
                continue;
            }
            assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("f", entry.getValue()).get().status());
            List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
            assertEquals(type + " should write one row", 1, rows.size());
            Object column = rows.get(0).get("f");
            if (column == null) {
                // Distinguish "arrays are broken for this type" from "this type writes no column at
                // all", which is a pre-existing storage gap rather than anything to do with arrays.
                if (writesAScalarColumn(type, entry.getValue().get(0))) {
                    unwired.add(type + " (writes a scalar column but not a list)");
                }
                continue;
            }
            if (isListColumnPlaceholder(column) == false) {
                unwired.add(type + " (column is not a LIST: " + column + ")");
            }
        }
        assertEquals("types accepting multi_value without storing a list", List.of(), unwired);
    }

    /** Whether this type writes a column at all when mapped single-valued. */
    private boolean writesAScalarColumn(String type, Object value) throws Exception {
        String indexName = "test-scalarbase-" + type.replace('_', '-');
        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(parquetPrimaryLuceneSecondarySettings())
                .setMapping("f", "type=" + type)
                .get()
                .isAcknowledged()
        );
        ensureGreen(indexName);
        assertEquals(RestStatus.CREATED, client().prepareIndex(indexName).setSource("f", value).get().status());
        List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
        return rows.size() == 1 && rows.get(0).get("f") != null;
    }

    /**
     * A realistic OpenTelemetry log record whose only complication is array-valued attributes — no
     * nested objects, no arrays of objects. Several semantic conventions are defined as arrays
     * ({@code http.request.header.<key>} and {@code process.command_args} are both {@code string[]}),
     * and they arrive under the wildcard {@code attributes.*} that no template can enumerate.
     *
     * <p>Uses the dynamic templates from data-prepper's logs-otel-v1 index template, so the mapping
     * decisions here are the ones a real OTel pipeline would produce.
     */
    public void testOtelLogRecordWithArrayAttributes() throws Exception {
        String indexName = "test-otel-logs";
        String mapping = "{"
            + "\"date_detection\":false,"
            + "\"dynamic_templates\":["
            + "{\"long_attributes\":{\"path_match\":\"attributes.*\",\"match_mapping_type\":\"long\","
            + "\"mapping\":{\"type\":\"long\"}}},"
            + "{\"double_attributes\":{\"path_match\":\"attributes.*\",\"match_mapping_type\":\"double\","
            + "\"mapping\":{\"type\":\"double\"}}},"
            + "{\"string_attributes\":{\"path_match\":\"attributes.*\",\"match_mapping_type\":\"string\","
            + "\"mapping\":{\"type\":\"keyword\",\"ignore_above\":256}}}"
            + "],"
            + "\"properties\":{"
            + "\"@timestamp\":{\"type\":\"date_nanos\"},"
            + "\"body\":{\"type\":\"text\"},"
            + "\"traceId\":{\"type\":\"keyword\"}"
            + "}}";
        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(parquetPrimaryLuceneSecondarySettings())
                .setMapping(mapping)
                .get()
                .isAcknowledged()
        );
        ensureGreen(indexName);

        // Attribute keys are flattened to one path segment, which is what keeps `attributes.*` matching.
        Map<String, Object> attributes = new java.util.LinkedHashMap<>();
        attributes.put("http@request@header@accept", List.of("application/json", "text/html"));
        attributes.put("process@command_args", List.of("python", "app.py", "--port", "8080"));
        attributes.put("http@response@status_code", 200);
        attributes.put("http@route", "/cart");

        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(indexName)
                .setSource(
                    "@timestamp",
                    "2026-01-01T00:00:00.000000000Z",
                    "body",
                    "GET /cart 200",
                    "traceId",
                    "0af7651916cd43dd8448eb211c80319c",
                    "attributes",
                    attributes
                )
                .get()
                .status()
        );

        Map<String, Object> mapped = nestedFieldMapping(indexName, "attributes");
        for (String arrayAttribute : List.of("http@request@header@accept", "process@command_args")) {
            assertEquals(arrayAttribute + " arrived as an array", "keyword", fieldOf(mapped, arrayAttribute).get("type"));
            assertEquals(
                arrayAttribute + " should be declared multi-valued",
                Boolean.TRUE,
                fieldOf(mapped, arrayAttribute).get("multi_value")
            );
            assertEquals("the template's own settings still apply", 256, fieldOf(mapped, arrayAttribute).get("ignore_above"));
        }
        for (String scalarAttribute : List.of("http@response@status_code", "http@route")) {
            assertFalse(
                scalarAttribute + " arrived as a scalar and must stay single-valued",
                fieldOf(mapped, scalarAttribute).containsKey("multi_value")
            );
        }

        List<Map<String, Object>> rows = refreshFlushAndReadParquetRows(indexName);
        assertEquals(1, rows.size());
        Map<String, Object> row = rows.get(0);
        assertTrue(isListColumnPlaceholder(row.get("attributes.http@request@header@accept")));
        assertTrue(isListColumnPlaceholder(row.get("attributes.process@command_args")));
        assertEquals(200L, ((Number) row.get("attributes.http@response@status_code")).longValue());
        assertEquals("/cart", row.get("attributes.http@route"));
        assertEquals("0af7651916cd43dd8448eb211c80319c", row.get("traceId"));
    }

    /**
     * KNOWN GAP, and a larger one than multi-value. The OpenTelemetry span shape declares
     * {@code events} and {@code links} as {@code nested}, and the pluggable data format rejects
     * {@code nested} outright at mapping time. So an OTel span cannot be ingested into a Parquet index
     * at all today, independent of any array work — a span's array-valued <em>attributes</em> are fine
     * (see {@link #testOtelLogRecordWithArrayAttributes}), but its event and link structures are not.
     */
    public void testOtelSpanWithNestedEventsIsRejected() throws Exception {
        String indexName = "test-otel-span-nested";
        String mapping = "{\"properties\":{"
            + "\"traceId\":{\"type\":\"keyword\"},"
            + "\"spanId\":{\"type\":\"keyword\"},"
            + "\"name\":{\"type\":\"keyword\"},"
            + "\"events\":{\"type\":\"nested\",\"properties\":{"
            + "\"time\":{\"type\":\"date_nanos\"},\"name\":{\"type\":\"keyword\"}}}"
            + "}}";
        Exception error = expectThrows(
            Exception.class,
            () -> client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(parquetPrimaryLuceneSecondarySettings())
                .setMapping(mapping)
                .get()
        );
        assertThat(
            org.opensearch.ExceptionsHelper.stackTrace(error),
            org.hamcrest.Matchers.containsString("nested type is not supported with pluggable data format")
        );
    }

    private void createParquetIndex(String indexName) {
        assertTrue(
            client().admin().indices().prepareCreate(indexName).setSettings(parquetPrimaryLuceneSecondarySettings()).get().isAcknowledged()
        );
        ensureGreen(indexName);
    }

    /** The mapping's top-level {@code properties}, empty when the index has no mapped fields yet. */
    @SuppressWarnings("unchecked")
    private Map<String, Object> clusterStateProperties(String indexName) {
        MappingMetadata mapping = getClusterState().metadata().index(indexName).mapping();
        if (mapping == null) {
            return Map.of();
        }
        Map<String, Object> properties = (Map<String, Object>) mapping.sourceAsMap().get("properties");
        return properties == null ? Map.of() : properties;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> nestedFieldMapping(String indexName, String objectFieldName) {
        return (Map<String, Object>) clusterStateFieldMapping(indexName, objectFieldName).get("properties");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> fieldOf(Map<String, Object> properties, String fieldName) {
        return (Map<String, Object>) properties.get(fieldName);
    }

    /**
     * RustBridge's test-only JSON renderer decodes primitive columns and emits this marker for
     * nested columns. Matching it verifies that the physical Parquet column is LIST; element-value
     * preservation is covered by the lower-level VSR and ParquetDocumentInput tests.
     */
    private boolean isListColumnPlaceholder(Object value) {
        return value instanceof String text && text.startsWith("<unsupported:List(");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> clusterStateFieldMapping(String indexName, String fieldName) {
        Map<String, Object> mappingSource = getClusterState().metadata().index(indexName).mapping().sourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) mappingSource.get("properties");
        return (Map<String, Object>) properties.get(fieldName);
    }

    private void refreshAndFlush(String indexName) {
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();
    }

    private List<Map<String, Object>> refreshFlushAndReadParquetRows(String indexName) throws IOException {
        refreshAndFlush(indexName);
        IndexShard shard = getIndexShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        try (GatedCloseable<List<Path>> parquetFilesRef = listParquetFiles(parquetDir, shard)) {
            return readAllParquetRows(parquetFilesRef.get());
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: index settings
    // ══════════════════════════════════════════════════════════════════════

    private Settings parquetOnlySettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();
    }

    private Settings parquetPrimaryLuceneSecondarySettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: indexing
    // ══════════════════════════════════════════════════════════════════════

    private void indexDocsWithDynamicFields(String indexName, int from, int to) {
        for (int i = from; i < to; i++) {
            IndexResponse response = client().prepareIndex()
                .setIndex(indexName)
                .setSource(
                    "field_keyword",
                    "value_" + i,
                    "field_number",
                    i,
                    "dynamic_text",
                    "dynamic_value_" + i,
                    "dynamic_long",
                    (long) i * 1000
                )
                .get();
            assertEquals(RestStatus.CREATED, response.status());
        }
    }

    private void runConcurrentIndexing(String indexName, int numThreads) throws Throwable {
        final Thread[] indexThreads = new Thread[numThreads];
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            indexThreads[i] = new Thread(() -> {
                try {
                    startLatch.await();
                    IndexResponse respA = client().prepareIndex(indexName).setSource("fieldA_" + threadId, "valueA_" + threadId).get();
                    assert respA.status() == RestStatus.CREATED : "index a_" + threadId + " failed: " + respA.status();
                    Thread.sleep(1000);
                    IndexResponse respB = client().prepareIndex(indexName).setSource("fieldB_" + threadId, "valueB_" + threadId).get();
                    assert respB.status() == RestStatus.CREATED : "index b_" + threadId + " failed: " + respB.status();
                    Thread.sleep(1000);
                    client().admin().indices().prepareRefresh(indexName).get();
                } catch (Exception e) {
                    error.compareAndSet(null, e);
                }
            });
            indexThreads[i].start();
        }
        startLatch.countDown();
        for (Thread thread : indexThreads) {
            thread.join();
        }
        if (error.get() != null) {
            throw error.get();
        }

        // Final refresh + flush
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();
    }

    // Wait for any in-flight merges to complete to avoid file handle leaks when the cluster shuts down while merge threads still hold open
    // readers.
    // The best way to do so is to trigger a force merge to single segment and ensure it completes so that no merges will happen again until
    // non new documents are ingested
    private void ensureNoActiveMerges(String indexName) throws IOException {
        try {
            assertBusy(() -> {
                try {
                    client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get();
                } catch (Exception e) {
                    // forceMerge throws if background merges are active — retry
                }
                try (GatedCloseable<CatalogSnapshot> cs = getIndexShard(indexName).getCatalogSnapshot()) {
                    assertEquals("Segment count after force merge should be 1", 1, cs.get().getSegments().size());
                }
            });
        } catch (Exception e) {
            throw new IOException("Timed out waiting for merges to complete", e);
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: shard access
    // ══════════════════════════════════════════════════════════════════════

    private IndexShard getIndexShard(String indexName) {
        return getIndexShard(internalCluster().getDataNodeNames().iterator().next(), new ShardId(resolveIndex(indexName), 0), indexName);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: parquet verification
    // ══════════════════════════════════════════════════════════════════════

    private GatedCloseable<List<Path>> listParquetFiles(Path parquetDir, IndexShard shard) throws IOException {
        assertTrue("Parquet directory should exist", Files.isDirectory(parquetDir));
        GatedCloseable<CatalogSnapshot> snapshot = shard.getCatalogSnapshot();
        List<Path> paths = new ArrayList<>();
        for (Segment segment : snapshot.get().getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            if (wfs != null) {
                for (String file : wfs.files()) {
                    paths.add(parquetDir.resolve(file));
                }
            }
        }
        return new GatedCloseable<>(paths, snapshot::close);
    }

    private long getParquetRowCount(List<Path> parquetFiles) throws IOException {
        long totalRows = 0;
        for (Path pf : parquetFiles) {
            ParquetFileMetadata meta = RustBridge.getFileMetadata(pf.toString());
            assertNotNull("Parquet file metadata should not be null: " + pf, meta);
            totalRows += meta.numRows();
        }
        return totalRows;
    }

    @SuppressForbidden(reason = "JSON parsing for test verification of parquet output")
    private List<Map<String, Object>> readAllParquetRows(List<Path> parquetFiles) throws IOException {
        List<Map<String, Object>> allRows = new ArrayList<>();
        for (Path pf : parquetFiles) {
            allRows.addAll(parseJsonRows(RustBridge.readAsJson(pf.toString())));
        }
        return allRows;
    }

    @SuppressWarnings("unchecked")
    @SuppressForbidden(reason = "JSON parsing for test verification of parquet output")
    private List<Map<String, Object>> parseJsonRows(String json) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json
            )
        ) {
            return parser.list().stream().map(o -> (Map<String, Object>) o).collect(Collectors.toList());
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: lucene verification
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Reads all documents from a Lucene index directory, extracting doc values fields
     * into a list of maps (one map per document).
     */
    private List<Map<String, Object>> readAllLuceneDocs(Path luceneDir) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                LeafReader leaf = ctx.reader();
                for (int doc = 0; doc < leaf.maxDoc(); doc++) {
                    Map<String, Object> row = new HashMap<>();
                    for (FieldInfo fi : leaf.getFieldInfos()) {
                        if (fi.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
                            SortedNumericDocValues dv = leaf.getSortedNumericDocValues(fi.name);
                            if (dv != null && dv.advanceExact(doc)) {
                                row.put(fi.name, dv.nextValue());
                            }
                        } else if (fi.getDocValuesType() == DocValuesType.SORTED_SET) {
                            SortedSetDocValues dv = leaf.getSortedSetDocValues(fi.name);
                            if (dv != null && dv.advanceExact(doc)) {
                                long ord = dv.nextOrd();
                                if (ord >= 0) {
                                    row.put(fi.name, dv.lookupOrd(ord).utf8ToString());
                                }
                            }
                        }
                    }
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: assertions
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Asserts that the given fields are present in the index mapping, polling with assertBusy
     * to account for the cluster-manager applying its own cluster state after publication completes.
     */
    private void assertMappingsContain(String indexName, String... expectedFields) throws Exception {
        assertBusy(() -> {
            GetMappingsResponse mappings = client().admin().indices().prepareGetMappings(indexName).get();
            Map<String, Object> mappingSource = mappings.getMappings().get(indexName).sourceAsMap();
            @SuppressWarnings("unchecked")
            Map<String, Object> properties = (Map<String, Object>) mappingSource.get("properties");
            for (String field : expectedFields) {
                assertTrue("Mapping should contain field '" + field + "'", properties.containsKey(field));
            }
        });
    }

    private void assertDynamicFieldCount(List<Map<String, Object>> rows, String fieldName, long expectedCount) {
        long count = rows.stream().filter(row -> row.containsKey(fieldName) && row.get(fieldName) != null).count();
        assertEquals(expectedCount + " rows should have " + fieldName + " field populated", expectedCount, count);
    }

    private void assertConcurrentMappings(String indexName, int numThreads) throws Exception {
        String[] expectedFields = new String[numThreads * 2];
        for (int i = 0; i < numThreads; i++) {
            expectedFields[i * 2] = "fieldA_" + i;
            expectedFields[i * 2 + 1] = "fieldB_" + i;
        }
        assertMappingsContain(indexName, expectedFields);
    }

    private void assertConcurrentFieldValues(List<Map<String, Object>> rows, int numThreads) {
        Set<String> foundA = new HashSet<>();
        Set<String> foundB = new HashSet<>();
        for (Map<String, Object> row : rows) {
            for (int i = 0; i < numThreads; i++) {
                if (("valueA_" + i).equals(row.get("fieldA_" + i))) foundA.add("fieldA_" + i);
                if (("valueB_" + i).equals(row.get("fieldB_" + i))) foundB.add("fieldB_" + i);
            }
        }
        assertEquals("All 32 fieldA values should be present", numThreads, foundA.size());
        assertEquals("All 32 fieldB values should be present", numThreads, foundB.size());
    }

    /**
     * Asserts that the given fields exist in the Lucene index as indexed fields (inverted index).
     * The Lucene secondary stores inverted indexes for search but not doc values for field values.
     */
    private void assertLuceneIndexedFieldsPresent(Path luceneDir, Set<String> expectedFields) throws IOException {
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            Set<String> allFields = new HashSet<>();
            for (LeafReaderContext ctx : reader.leaves()) {
                for (FieldInfo fi : ctx.reader().getFieldInfos()) {
                    allFields.add(fi.name);
                }
            }
            for (String expected : expectedFields) {
                assertTrue(
                    "Lucene index should contain field '" + expected + "', found fields: " + allFields,
                    allFields.contains(expected)
                );
            }
        }
    }
}
