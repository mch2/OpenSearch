/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Benchmark harness for the doc-values leaf PoC (spec: "the PoC's output is numbers, not just
 * code"). Ingests a hits-style dataset into a lucene-primary (doc-values) index and a
 * parquet-primary baseline, then times the spec's four query shapes on both through the SAME
 * distributed engine, plus classic {@code _search} aggs on the DV index (the engine term).
 *
 * <p>Gated behind {@code -Dtests.dv_bench=true} so the normal suite skips it. Keyword-mode A/B:
 * run once on the default cluster (utf8) and once via {@code integTestDvDictionary} (dictionary).
 * Numbers print to stdout as a markdown table; wall times are medians of {@code RUNS} after one
 * warmup.
 */
public class DocValuesLeafBenchIT extends AnalyticsRestTestCase {

    private static final String DV_INDEX = "dv_bench_hits";
    private static final String PARQUET_INDEX = "dv_bench_hits_parquet";
    private static final String FLAG = "analytics.query.distributed_engine";
    private static final int DOCS = 200_000;
    private static final int RUNS = 5;

    public void testBenchmark() throws Exception {
        assumeTrue("benchmark runs only with -Dtests.dv_bench=true", "true".equals(System.getProperty("tests.dv_bench")));

        provision(DV_INDEX, "lucene");
        provision(PARQUET_INDEX, "parquet");

        // The spec's four shapes. counter_class has ~10 distinct values; url_hash ~50k (the
        // high-cardinality string group-by — the item-9 instrument); watch_id is a wide long.
        String[][] shapes = {
            { "selective+groupby", "| where counter_class = 'c3' | stats count() as n, sum(duration) as d by region | sort region" },
            { "lowsel+groupby", "| where duration > 10 | stats count() as n, avg(duration) as d by region | sort region" },
            { "highcard-string", "| stats count() as n by url_hash | sort - n | head 10" },
            { "fullscan-agg", "| stats count() as n, sum(duration) as total, min(watch_id) as lo, max(watch_id) as hi" } };

        StringBuilder report = new StringBuilder();
        report.append("\n=== dv-leaf benchmark (docs=").append(DOCS).append(", runs=").append(RUNS).append(", median ms) ===\n");
        report.append(String.format(Locale.ROOT, "%-20s %12s %12s %12s%n", "shape", "dv-leaf", "parquet", "search-agg"));

        setDistributedEngine(true);
        try {
            for (String[] shape : shapes) {
                long dv = timePpl("source = " + DV_INDEX + " " + shape[1]);
                long pq = timePpl("source = " + PARQUET_INDEX + " " + shape[1]);
                long search = timeSearchAgg(shape[0]);
                report.append(
                    String.format(
                        Locale.ROOT,
                        "%-20s %10dms %10dms %12s%n",
                        shape[0],
                        dv,
                        pq,
                        search < 0 ? "n/a" : search + "ms"
                    )
                );
            }
        } finally {
            setDistributedEngine(false);
        }
        logger.info(report.toString());
        System.out.println(report);
    }

    /** Median wall time of RUNS executions (1 warmup discarded). */
    private long timePpl(String ppl) throws Exception {
        executePpl(ppl); // warmup
        List<Long> times = new ArrayList<>(RUNS);
        for (int i = 0; i < RUNS; i++) {
            long start = System.nanoTime();
            executePpl(ppl);
            times.add((System.nanoTime() - start) / 1_000_000);
        }
        times.sort(null);
        return times.get(RUNS / 2);
    }

    /** Classic _search agg equivalent on the DV index (the "engine term" comparison); -1 if unsupported. */
    private long timeSearchAgg(String shape) {
        String body = switch (shape) {
            case "selective+groupby" -> "{\"size\":0,\"query\":{\"term\":{\"counter_class\":\"c3\"}},"
                + "\"aggs\":{\"g\":{\"terms\":{\"field\":\"region\"},\"aggs\":{\"d\":{\"sum\":{\"field\":\"duration\"}}}}}}";
            case "lowsel+groupby" -> "{\"size\":0,\"query\":{\"range\":{\"duration\":{\"gt\":10}}},"
                + "\"aggs\":{\"g\":{\"terms\":{\"field\":\"region\"},\"aggs\":{\"d\":{\"avg\":{\"field\":\"duration\"}}}}}}";
            case "highcard-string" -> "{\"size\":0,\"aggs\":{\"g\":{\"terms\":{\"field\":\"url_hash\",\"size\":10}}}}";
            case "fullscan-agg" -> "{\"size\":0,\"aggs\":{\"n\":{\"value_count\":{\"field\":\"watch_id\"}},"
                + "\"total\":{\"sum\":{\"field\":\"duration\"}},\"lo\":{\"min\":{\"field\":\"watch_id\"}},\"hi\":{\"max\":{\"field\":\"watch_id\"}}}}";
            default -> null;
        };
        if (body == null) {
            return -1;
        }
        try {
            Request warm = new Request("POST", "/" + DV_INDEX + "/_search");
            warm.setJsonEntity(body);
            client().performRequest(warm);
            List<Long> times = new ArrayList<>(RUNS);
            for (int i = 0; i < RUNS; i++) {
                Request req = new Request("POST", "/" + DV_INDEX + "/_search");
                req.setJsonEntity(body);
                long start = System.nanoTime();
                client().performRequest(req);
                times.add((System.nanoTime() - start) / 1_000_000);
            }
            times.sort(null);
            return times.get(RUNS / 2);
        } catch (Exception e) {
            logger.info("classic _search agg unsupported on " + DV_INDEX + ": " + e.getMessage());
            return -1;
        }
    }

    private void setDistributedEngine(boolean on) throws Exception {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"" + FLAG + "\": " + on + "}}");
        assertOkAndParse(client().performRequest(req), "set " + FLAG + "=" + on);
    }

    private void provision(String name, String primaryFormat) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + name));
        } catch (Exception ignored) {}
        String secondary = primaryFormat.equals("parquet") ? "\"index.composite.secondary_data_formats\": [\"lucene\"]," : "";
        String body = String.format(
            Locale.ROOT,
            "{"
                + "\"settings\": {"
                + "  \"number_of_shards\": 2, \"number_of_replicas\": 0, \"refresh_interval\": \"-1\","
                + "  \"index.pluggable.dataformat.enabled\": true,"
                + "  \"index.pluggable.dataformat\": \"composite\","
                + "  \"index.composite.primary_data_format\": \"%s\","
                + "  %s"
                + "  \"index.translog.durability\": \"async\""
                + "},"
                + "\"mappings\": {\"properties\": {"
                + "  \"region\":        { \"type\": \"keyword\" },"
                + "  \"counter_class\": { \"type\": \"keyword\" },"
                + "  \"url_hash\":      { \"type\": \"keyword\" },"
                + "  \"duration\":      { \"type\": \"long\" },"
                + "  \"watch_id\":      { \"type\": \"long\" },"
                + "  \"ts\":            { \"type\": \"date\" }"
                + "}}}",
            primaryFormat,
            secondary
        );
        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity(body);
        assertOkAndParse(client().performRequest(create), "create " + name);
        Request health = new Request("GET", "/_cluster/health/" + name);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "60s");
        client().performRequest(health);

        // Deterministic hits-style rows: 12 regions, 10 counter classes, ~DOCS/4 distinct url
        // hashes, wide watch_id longs (the gcd/delta lesson: wide values, no tidy encoding).
        StringBuilder bulk = new StringBuilder(1 << 22);
        int flushed = 0;
        for (int i = 0; i < DOCS; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"region\":\"r")
                .append(i % 12)
                .append("\",\"counter_class\":\"c")
                .append(i % 10)
                .append("\",\"url_hash\":\"u")
                .append((i * 2654435761L) % (DOCS / 4))
                .append("\",\"duration\":")
                .append((i * 37) % 2000)
                .append(",\"watch_id\":")
                .append(i * 6364136223846793005L)
                .append(",\"ts\":")
                .append(1700000000000L + i * 100L)
                .append("}\n");
            if ((i + 1) % 10_000 == 0 || i == DOCS - 1) {
                Request bulkRequest = new Request("POST", "/" + name + "/_bulk");
                bulkRequest.setJsonEntity(bulk.toString());
                Map<String, Object> resp = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + name + " wave " + flushed);
                assertEquals("bulk wave " + flushed + " must not error", Boolean.FALSE, resp.get("errors"));
                bulk.setLength(0);
                flushed++;
            }
        }
        client().performRequest(new Request("POST", "/" + name + "/_refresh"));
        client().performRequest(new Request("POST", "/" + name + "/_flush?force=true"));
    }
}
