/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * {@code mvexpand} and per-element {@code stats … by <array>} over multi-valued Parquet columns.
 *
 * <p>The expansion lowers to a Calcite {@code Correlate} + {@code Uncollect}, crosses the wire as a
 * Substrait {@code ExtensionSingleRel} (Substrait has no unnest relation and
 * {@code datafusion-substrait} implements neither direction), and is rebuilt on the shard as
 * DataFusion's {@code LogicalPlan::Unnest}. So these assertions cover a path that is custom at every
 * layer — the schema must type the column {@code ARRAY}, the marking rules must convert both rels,
 * the backend must declare {@code MULTI_VALUE_EXPAND}, and every consumer entry point must be ours.
 *
 * <p><b>Element order is not stable.</b> Repeated runs of the same query returned a document's
 * elements in different orders, so every assertion here compares row multisets rather than sequences.
 *
 * <p>Fixture — array-ness is left to dynamic detection, so the first document must present each
 * field as an array for it to be declared multi-valued:
 *
 * <pre>
 * id  serviceName  durationMs  tags                          codes       events.name
 * s1  checkout     120         [prod, us-east]               [200, 201]  [start, validate, commit]
 * s2  payment      450         [prod, us-west]               [500, 503]  [start, timeout]
 * s3  cart          30         [staging, us-west, canary]    [200]       [start]
 * s4  checkout     900         [prod]                        [500]       [start, retry, retry, fail]
 * s5  search        75         []            (empty array)   [200]       (absent)
 * s6  quote         60         (absent)                      (absent)    (absent)
 * </pre>
 */
public class MvExpandIT extends AnalyticsRestTestCase {

    private static final String INDEX = "mvexpand_it";

    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0}}"
        );
        client().performRequest(create);
        Request bulk = new Request("POST", "/" + INDEX + "/_bulk?refresh=true");
        // No mapping: multi_value is stamped by dynamic mapping from the enclosing array in the
        // document that creates each field, so the multi-element document has to come first.
        bulk.setJsonEntity(
            "{\"index\":{}}\n{\"id\":\"s1\",\"serviceName\":\"checkout\",\"durationMs\":120,"
                + "\"tags\":[\"prod\",\"us-east\"],\"codes\":[200,201],"
                + "\"events\":[{\"name\":\"start\"},{\"name\":\"validate\"},{\"name\":\"commit\"}]}\n"
                + "{\"index\":{}}\n{\"id\":\"s2\",\"serviceName\":\"payment\",\"durationMs\":450,"
                + "\"tags\":[\"prod\",\"us-west\"],\"codes\":[500,503],"
                + "\"events\":[{\"name\":\"start\"},{\"name\":\"timeout\"}]}\n"
                + "{\"index\":{}}\n{\"id\":\"s3\",\"serviceName\":\"cart\",\"durationMs\":30,"
                + "\"tags\":[\"staging\",\"us-west\",\"canary\"],\"codes\":[200],"
                + "\"events\":[{\"name\":\"start\"}]}\n"
                + "{\"index\":{}}\n{\"id\":\"s4\",\"serviceName\":\"checkout\",\"durationMs\":900,"
                + "\"tags\":[\"prod\"],\"codes\":[500],"
                + "\"events\":[{\"name\":\"start\"},{\"name\":\"retry\"},{\"name\":\"retry\"},{\"name\":\"fail\"}]}\n"
                + "{\"index\":{}}\n{\"id\":\"s5\",\"serviceName\":\"search\",\"durationMs\":75,\"tags\":[],\"codes\":[200]}\n"
                + "{\"index\":{}}\n{\"id\":\"s6\",\"serviceName\":\"quote\",\"durationMs\":60}\n"
        );
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);
        provisioned = true;
    }

    /** The mapping must declare the array fields, or nothing below can work. */
    @SuppressWarnings("unchecked")
    public void testDynamicMappingDeclaresTheArrayFields() throws IOException {
        Map<String, Object> mapping = entityAsMap(client().performRequest(new Request("GET", "/" + INDEX + "/_mapping")));
        Map<String, Object> props = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) mapping.get(INDEX)).get("mappings")).get(
            "properties"
        );
        assertEquals("tags must be multi-valued", Boolean.TRUE, ((Map<String, Object>) props.get("tags")).get("multi_value"));
        assertEquals("codes must be multi-valued", Boolean.TRUE, ((Map<String, Object>) props.get("codes")).get("multi_value"));
        // Scalars must be left alone — stamping every field would change existing mapping output.
        assertNull("serviceName must stay scalar", ((Map<String, Object>) props.get("serviceName")).get("multi_value"));
        assertNull("durationMs must stay scalar", ((Map<String, Object>) props.get("durationMs")).get("multi_value"));
    }

    /** One row per element of a text array; the scalar column repeats alongside it. */
    public void testExpandTextArray() throws IOException {
        assertRowsEqualUnordered(
            "source=" + INDEX + " | fields id, tags | mvexpand tags",
            row("s1", "prod"),
            row("s1", "us-east"),
            row("s2", "prod"),
            row("s2", "us-west"),
            row("s3", "staging"),
            row("s3", "us-west"),
            row("s3", "canary"),
            row("s4", "prod"),
            row("s6", null)
        );
    }

    /** Same for a numeric array — the element type survives, values are not stringified. */
    public void testExpandLongArray() throws IOException {
        assertRowsEqualUnordered(
            "source=" + INDEX + " | fields id, codes | mvexpand codes",
            row("s1", 200),
            row("s1", 201),
            row("s2", 500),
            row("s2", 503),
            row("s3", 200),
            row("s4", 500),
            row("s5", 200),
            row("s6", null)
        );
    }

    /** A leaf under an object, which is where OTel span events land. */
    public void testExpandLeafUnderAnObject() throws IOException {
        assertRowsEqualUnordered(
            "source=" + INDEX + " | fields id, events.name | mvexpand events.name",
            row("s1", "start"),
            row("s1", "validate"),
            row("s1", "commit"),
            row("s2", "start"),
            row("s2", "timeout"),
            row("s3", "start"),
            row("s4", "start"),
            row("s4", "retry"),
            row("s4", "retry"),
            row("s4", "fail"),
            row("s5", null),
            row("s6", null)
        );
    }

    /**
     * Duplicates are preserved. The Substrait payload carries a {@code distinct} flag, so a wrong
     * default here would silently collapse {@code s4}'s two {@code retry} events into one.
     */
    public void testDuplicateElementsArePreserved() throws IOException {
        assertRowsEqualUnordered("source=" + INDEX + " | fields id, events.name | mvexpand events.name | where id = 's4' | stats count()", row(4));
    }

    /** {@code limit} caps elements per document — s3 keeps 2 of its 3 tags, others are unaffected. */
    public void testExpandRespectsLimit() throws IOException {
        assertRowsEqualUnordered("source=" + INDEX + " | fields id, tags | mvexpand tags limit=2 | stats count()", row(8));
        assertRowsEqualUnordered("source=" + INDEX + " | fields id, tags | mvexpand tags limit=2 | where id = 's3' | stats count()", row(2));
    }

    /** A scalar column survives the expansion, so it can be aggregated per element. */
    public void testAggregateScalarPerElement() throws IOException {
        assertRowsEqualUnordered(
            "source=" + INDEX + " | fields serviceName, durationMs, tags | mvexpand tags | stats avg(durationMs) by tags",
            row(490.0, "prod"),      // (120 + 450 + 900) / 3
            row(120.0, "us-east"),
            row(240.0, "us-west"),   // (450 + 30) / 2
            row(30.0, "staging"),
            row(30.0, "canary"),
            row(60.0, null)
        );
    }

    /**
     * {@code stats … by <array>} explodes implicitly — no {@code mvexpand} needed — giving each
     * element its own bucket, which is Lucene terms-agg parity. Same buckets as the explicit form.
     */
    public void testGroupByArrayExplodesImplicitly() throws IOException {
        List<List<Object>> expected = List.of(
            row(3, "prod"),
            row(2, "us-west"),
            row(1, "us-east"),
            row(1, "staging"),
            row(1, "canary"),
            rowWithNull(1)
        );
        assertRowsEqualUnordered("source=" + INDEX + " | stats count() by tags", expected);
        assertRowsEqualUnordered("source=" + INDEX + " | fields tags | mvexpand tags | stats count() by tags", expected);
    }

    /** {@link List#of} rejects nulls, so the null-key bucket needs Arrays.asList. */
    private static List<Object> rowWithNull(Object first) {
        return Arrays.asList(first, null);
    }

    /**
     * Pins an asymmetry worth knowing about: an <em>empty</em> array contributes no row, while an
     * <em>absent</em> field contributes one row holding null.
     *
     * <p>s5 has {@code "tags": []} and does not appear at all; s6 has no {@code tags} key and appears
     * once as null. Standard SQL {@code UNNEST} drops both, and Splunk's {@code mvexpand} drops both,
     * so the null row for s6 is a divergence — it comes from unnesting a null with nulls preserved.
     * Recorded rather than endorsed; if it is ever made consistent, this is the test to change.
     */
    public void testEmptyArrayIsDroppedButAbsentFieldYieldsNull() throws IOException {
        assertRowsEqualUnordered("source=" + INDEX + " | fields id, tags | mvexpand tags | stats count()", row(9));
        assertRowsEqualUnordered("source=" + INDEX + " | fields id, tags | mvexpand tags | where isnull(tags) | fields id", row("s6"));
    }

    /**
     * Pins a second asymmetry, and this one looks like a defect: an empty array is counted by
     * neither {@code isnull} nor {@code isnotnull}.
     *
     * <p>Six documents, but 1 + 4 = 5. s5's {@code "tags": []} falls through both predicates. Kept as
     * an assertion so the behaviour is visible and a fix is forced to update it.
     */
    public void testEmptyArrayMatchesNeitherNullPredicate() throws IOException {
        assertRowsEqualUnordered("source=" + INDEX + " | where isnull(tags) | fields id", row("s6"));
        assertRowsEqualUnordered("source=" + INDEX + " | where isnotnull(tags) | stats count()", row(4));
        assertRowsEqualUnordered("source=" + INDEX + " | stats count()", row(6));
    }

    /**
     * Filtering on the expanded field. <b>Currently fails</b>, and it is the plainest possible use of
     * the command.
     *
     * <p>Adding the filter makes Calcite push {@code LogicalFilter} into the correlate's right input,
     * so the plan becomes {@code Correlate(scan, Filter(Uncollect(…)))} instead of
     * {@code Correlate(scan, Uncollect(…))}. {@code OpenSearchCorrelateRule} does not match that
     * shape, the correlate is never marked, and {@code OpenSearchProjectRule} throws
     * {@code Project rule encountered unmarked child [LogicalCorrelate]}.
     */
    public void testFilterOnExpandedTextField() throws IOException {
        assertRowsEqualUnordered(
            "source=" + INDEX + " | fields id, tags | mvexpand tags | where tags = 'prod'",
            row("s1", "prod"),
            row("s2", "prod"),
            row("s4", "prod")
        );
    }

    /** Same gap on a numeric comparison — see {@link #testFilterOnExpandedTextField()}. */
    public void testFilterOnExpandedLongField() throws IOException {
        assertRowsEqualUnordered(
            "source=" + INDEX + " | fields id, codes | mvexpand codes | where codes >= 500",
            row("s2", 500),
            row("s2", 503),
            row("s4", 500)
        );
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /**
     * Compares rows as a multiset. Element order within a document is not stable across runs, and
     * {@code stats} output order is unspecified, so sequence comparison would be flaky.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsEqualUnordered(String ppl, List<Object>... expected) throws IOException {
        assertRowsEqualUnordered(ppl, Arrays.asList(expected));
    }

    private void assertRowsEqualUnordered(String ppl, List<List<Object>> expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actual = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, actual);
        List<String> want = new ArrayList<>();
        for (List<Object> r : expected) {
            want.add(normalize(r));
        }
        List<String> got = new ArrayList<>();
        for (List<Object> r : actual) {
            got.add(normalize(r));
        }
        want.sort(null);
        got.sort(null);
        assertEquals("Rows mismatch for query: " + ppl, want, got);
    }

    /** Renders a row so numeric widths compare equal (a bigint may arrive as Integer or Long). */
    private static String normalize(List<Object> row) {
        StringBuilder sb = new StringBuilder();
        for (Object v : row) {
            sb.append('|');
            if (v == null) {
                sb.append("null");
            } else if (v instanceof Number n) {
                double d = n.doubleValue();
                sb.append(d == Math.rint(d) ? Long.toString((long) d) : Double.toString(d));
            } else {
                sb.append(v);
            }
        }
        return sb.toString();
    }
}
