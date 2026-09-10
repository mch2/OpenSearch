/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa.multivalue;

import org.opensearch.analytics.qa.AnalyticsRestTestCase;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runs every probe in {@link Probe#all()} against both the scalar and the multi-value column of
 * every type in {@link MultiValueDataset#TYPES}, and reports where the two disagree.
 *
 * <p>This is a measurement harness, not a correctness test. It does not assert expected values: the
 * point is to discover which of the engine's supported query shapes break — or silently change
 * meaning — when a field goes from single- to multi-valued. Failures are data, so a probe that
 * errors is recorded and the run continues. The single assertion at the end is that the report was
 * produced and that the scalar baseline itself is healthy; if the scalar side is broken the multi
 * side tells us nothing.
 *
 * <p>Output: a markdown report at {@code build/multivalue-matrix.md} plus the same content in the
 * test log. Read the {@code verdict} column first:
 * <ul>
 *   <li>{@code SAME} — the multi column answers exactly as the scalar column. Nothing to do.</li>
 *   <li>{@code DIFFERS} — both answer, differently. A semantics decision, not a bug: someone has to
 *       say which answer is right (group-by is the headline case).</li>
 *   <li>{@code MULTI_FAILS} — the shape works on scalar and errors on multi. Needs a planner rewrite
 *       or a kernel. This is the work list for Approach A.</li>
 *   <li>{@code SCALAR_FAILS} — works on multi, not on scalar. Expected for the array-native probes;
 *       anywhere else it means the probe is malformed.</li>
 *   <li>{@code BOTH_FAIL} — the shape is unsupported for the type regardless of cardinality. Out of
 *       scope for multi-value work.</li>
 * </ul>
 *
 * <p>Run with {@code -Danalytics.multivalue.surface_arrays=false} on the cluster to get the other
 * half of the picture: the Calcite schema then calls the column by its element type while the
 * parquet file holds a LIST, which separates front-end type-check failures from physical schema
 * failures. See {@code OpenSearchSchemaBuilder}.
 */
public class MultiValueFunctionMatrixIT extends AnalyticsRestTestCase {

    /** Where a probe stopped, inferred from the error text. Guides who owns the fix. */
    private enum FailureStage {
        /** PPL analysis or Calcite validation rejected the expression against the declared type. */
        FRONTEND,
        /** Calcite RelNode → Substrait conversion had no mapping for the call. */
        SUBSTRAIT,
        /** No backend volunteered to evaluate a delegated predicate. */
        DELEGATION,
        /** DataFusion logical planning: signature/coercion mismatch. */
        DF_PLAN,
        /** DataFusion execution: a kernel refused the argument at runtime. */
        DF_EXEC,
        /** Engine-internal Java error (schema conversion, stitching, row codec). */
        ENGINE,
        /**
         * The engine returned a 500 whose body is redacted to {@code Internal error [task_id=N]}.
         * The real exception is only in the node log — grep {@code integTest.log} for that task id.
         * Most multi-value read failures land here, so the report cannot be read on its own.
         */
        BACKEND_REDACTED,
        UNCLASSIFIED
    }

    private enum Verdict {
        SAME,
        DIFFERS,
        MULTI_FAILS,
        SCALAR_FAILS,
        BOTH_FAIL
    }

    /** One probe run: either rendered rows, or an error with the stage it came from. */
    private record Outcome(boolean ok, String rows, FailureStage stage, String error) {
        static Outcome success(String rows) {
            return new Outcome(true, rows, null, null);
        }

        static Outcome failure(FailureStage stage, String error) {
            return new Outcome(false, null, stage, error);
        }

        String cell() {
            return ok ? rows : stage + ": " + truncate(error, 90);
        }
    }

    private record Result(Probe probe, String osType, Outcome scalar, Outcome multi) {
        Verdict verdict() {
            if (scalar.ok() && multi.ok()) {
                return scalar.rows().equals(multi.rows()) ? Verdict.SAME : Verdict.DIFFERS;
            }
            if (scalar.ok()) {
                return Verdict.MULTI_FAILS;
            }
            return multi.ok() ? Verdict.SCALAR_FAILS : Verdict.BOTH_FAIL;
        }
    }

    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned == false) {
            MultiValueDataset.provision(client());
            provisioned = true;
        }
    }

    public void testFunctionMatrix() throws Exception {
        List<Result> results = new ArrayList<>();
        for (Probe probe : Probe.all()) {
            for (MultiValueDataset.TypeSpec type : MultiValueDataset.TYPES) {
                if (probe.appliesTo(type.osType()) == false) {
                    continue;
                }
                Probe.Literals literals = Probe.Literals.forType(type.osType());
                Outcome scalar = run(probe.render(MultiValueDataset.INDEX, type.scalarField(), literals));
                Outcome multi = type.multiValueSupported()
                    ? run(probe.render(MultiValueDataset.INDEX, type.multiField(), literals))
                    : Outcome.failure(FailureStage.ENGINE, "type has no parquet LIST writer; multi_value column not created");
                results.add(new Result(probe, type.osType(), scalar, multi));
            }
        }

        String report = render(results);
        logger.info("multi-value function matrix\n{}", report);
        write(report);

        List<String> unexpectedScalarFailures = results.stream()
            .filter(r -> r.scalar().ok() == false)
            .filter(r -> r.probe().category() != Probe.Category.ARRAY_NATIVE)
            .map(r -> r.probe().id() + "/" + r.osType())
            .filter(id -> KNOWN_SCALAR_FAILURES.contains(id) == false)
            .toList();
        // The scalar column is the control. A new failure there means the matrix is measuring a
        // broken baseline rather than multi-value behavior, so the run's conclusions don't hold.
        assertTrue(
            "new scalar-column failures invalidate the comparison (add to KNOWN_SCALAR_FAILURES only"
                + " after confirming they are unrelated to multi-value): "
                + unexpectedScalarFailures,
            unexpectedScalarFailures.isEmpty()
        );
    }

    /**
     * Probes that already fail against an ordinary single-valued column, so they carry no
     * multi-value signal. Each is an independent engine gap found while building this harness:
     * <ul>
     *   <li>{@code filter-eq/boolean} — equality on a scalar boolean column errors in the backend.</li>
     *   <li>{@code agg-distinct-count/double}, {@code agg-distinct-count/boolean} — distinct_count
     *       has no working path for those types.</li>
     *   <li>{@code agg-min-max/ip} — min/max over the ip UDT (VARBINARY) errors.</li>
     * </ul>
     * The array-native probes are excluded wholesale rather than listed: they are meaningless on a
     * scalar column by construction.
     */
    private static final java.util.Set<String> KNOWN_SCALAR_FAILURES = java.util.Set.of(
        "filter-eq/boolean",
        "agg-distinct-count/double",
        "agg-distinct-count/boolean",
        "agg-min-max/ip"
    );

    /**
     * Auto-promotion route: a field mapped as an ordinary scalar keyword that becomes multi-valued
     * because a document arrived with two values. Kept separate from the matrix because it produces
     * mixed physical generations (scalar-column files and LIST-column files in one shard), which is
     * a different failure mode than a uniformly-LIST index.
     */
    public void testAutoPromotionReadPath() throws Exception {
        String index = MultiValueDataset.promoteField(client(), "mv_promoted", "tags");

        Map<String, Object> mapping = entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_mapping")));
        logger.info("mapping after promotion: {}", mapping);

        List<String> lines = new ArrayList<>();
        for (String ppl : List.of(
            "source=" + index + " | fields id, tags | sort id",
            "source=" + index + " | where tags = 'alpha' | stats count()",
            "source=" + index + " | where tags = 'beta' | stats count()",
            "source=" + index + " | stats count() by tags | sort tags",
            "source=" + index + " | stats distinct_count(tags)"
        )) {
            Outcome outcome = run(ppl);
            lines.add("- `" + ppl + "` → " + outcome.cell());
        }
        String report = "## Auto-promotion (mixed scalar + LIST generations)\n\n" + String.join("\n", lines) + "\n";
        logger.info("auto-promotion read path\n{}", report);
        write("multivalue-promotion-" + arm() + ".md", report);
    }

    // ── execution ───────────────────────────────────────────────────────────────────────────────

    private Outcome run(String ppl) {
        try {
            Request request = new Request("POST", "/_plugins/_ppl");
            request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
            Response response = client().performRequest(request);
            return Outcome.success(rows(entityAsMap(response)));
        } catch (ResponseException e) {
            String body = body(e);
            return Outcome.failure(classify(body), reason(body));
        } catch (Exception e) {
            return Outcome.failure(FailureStage.UNCLASSIFIED, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /**
     * Renders {@code datarows} as a compact, order-stable string. Every probe either sorts or
     * aggregates to a single row, so string equality between the scalar and multi renderings is a
     * sound "same answer" test.
     */
    @SuppressWarnings("unchecked")
    private static String rows(Map<String, Object> response) {
        Object datarows = response.get("datarows");
        if (datarows == null) {
            return "<no datarows>";
        }
        List<List<Object>> rows = (List<List<Object>>) datarows;
        List<String> rendered = new ArrayList<>(rows.size());
        for (List<Object> row : rows) {
            rendered.add(String.valueOf(row));
        }
        return String.join(" ", rendered);
    }

    private static String body(ResponseException e) {
        try {
            return new String(e.getResponse().getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException io) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** Pulls the most specific message out of the error envelope without a JSON parse. */
    private static String reason(String body) {
        for (String key : List.of("\"details\":\"", "\"reason\":\"", "\"error\":\"")) {
            int start = body.indexOf(key);
            if (start >= 0) {
                start += key.length();
                int end = body.indexOf('"', start);
                while (end > start && body.charAt(end - 1) == '\\') {
                    end = body.indexOf('"', end + 1);
                }
                if (end > start) {
                    return body.substring(start, end);
                }
            }
        }
        return truncate(body, 300);
    }

    /**
     * Buckets an error by the layer that produced it, using text the layers already emit. Substring
     * matching is fragile by nature; the raw message stays in the report so a mis-bucketed row is
     * still readable.
     */
    private static FailureStage classify(String body) {
        String lower = body.toLowerCase(java.util.Locale.ROOT);
        if (lower.contains("internal error [task_id=")) {
            return FailureStage.BACKEND_REDACTED;
        }
        if (lower.contains("no backend can evaluate")) {
            return FailureStage.DELEGATION;
        }
        if (lower.contains("unable to convert") || lower.contains("substrait")) {
            return FailureStage.SUBSTRAIT;
        }
        // The sql plugin's PPLTypeChecker rejects an operand whose SqlTypeName is outside the
        // function's declared type family, which is what an ARRAY-typed column hits. Its messages
        // are "<fn> function expects {...}, but got ...", "Cannot resolve function: ...", and
        // "Aggregation function <fn> expects field type {...}, but got ..." — see PPLFuncImpTable.
        if (lower.contains("cannot apply")
            || lower.contains("syntaxcheckexception")
            || lower.contains("semanticcheckexception")
            || lower.contains("can't resolve")
            || lower.contains("cannot resolve function")
            || lower.contains("but got")
            || lower.contains("expressionevaluationexception")
            || lower.contains("no match found for function signature")) {
            return FailureStage.FRONTEND;
        }
        if (lower.contains("error during planning")
            || lower.contains("failed to coerce")
            || lower.contains("no function matches")
            || lower.contains("schema error")
            || lower.contains("type_coercion")) {
            return FailureStage.DF_PLAN;
        }
        if (lower.contains("arrow error") || lower.contains("execution error") || lower.contains("not implemented")) {
            return FailureStage.DF_EXEC;
        }
        if (lower.contains("unsupported calcite type")
            || lower.contains("illegalargumentexception")
            || lower.contains("illegalstateexception")
            || lower.contains("classcastexception")) {
            return FailureStage.ENGINE;
        }
        return FailureStage.UNCLASSIFIED;
    }

    // ── reporting ───────────────────────────────────────────────────────────────────────────────

    private static String render(List<Result> results) {
        StringBuilder out = new StringBuilder();
        out.append("# Multi-value function matrix\n\n");
        out.append("Surface arrays: `").append(System.getProperty("analytics.multivalue.surface_arrays", "true")).append("`\n\n");

        Map<Verdict, Integer> counts = new EnumMap<>(Verdict.class);
        for (Result r : results) {
            counts.merge(r.verdict(), 1, Integer::sum);
        }
        out.append("## Summary\n\n| verdict | count |\n|---|---|\n");
        for (Verdict v : Verdict.values()) {
            out.append("| ").append(v).append(" | ").append(counts.getOrDefault(v, 0)).append(" |\n");
        }

        out.append("\n## Work list (MULTI_FAILS)\n\n| probe | type | stage | error |\n|---|---|---|---|\n");
        for (Result r : results) {
            if (r.verdict() == Verdict.MULTI_FAILS) {
                out.append("| ")
                    .append(r.probe().id())
                    .append(" | ")
                    .append(r.osType())
                    .append(" | ")
                    .append(r.multi().stage())
                    .append(" | ")
                    .append(escapeCell(truncate(r.multi().error(), 160)))
                    .append(" |\n");
            }
        }

        out.append("\n## Semantics decisions (DIFFERS)\n\n| probe | type | scalar | multi |\n|---|---|---|---|\n");
        for (Result r : results) {
            if (r.verdict() == Verdict.DIFFERS) {
                out.append("| ")
                    .append(r.probe().id())
                    .append(" | ")
                    .append(r.osType())
                    .append(" | ")
                    .append(escapeCell(truncate(r.scalar().rows(), 120)))
                    .append(" | ")
                    .append(escapeCell(truncate(r.multi().rows(), 120)))
                    .append(" |\n");
            }
        }

        Map<Probe.Category, List<Result>> byCategory = new LinkedHashMap<>();
        for (Result r : results) {
            byCategory.computeIfAbsent(r.probe().category(), k -> new ArrayList<>()).add(r);
        }
        out.append("\n## Full matrix\n");
        for (Map.Entry<Probe.Category, List<Result>> entry : byCategory.entrySet()) {
            out.append("\n### ").append(entry.getKey()).append("\n\n| probe | type | verdict | scalar | multi |\n|---|---|---|---|---|\n");
            for (Result r : entry.getValue()) {
                out.append("| ")
                    .append(r.probe().id())
                    .append(" | ")
                    .append(r.osType())
                    .append(" | ")
                    .append(r.verdict())
                    .append(" | ")
                    .append(escapeCell(truncate(r.scalar().cell(), 110)))
                    .append(" | ")
                    .append(escapeCell(truncate(r.multi().cell(), 110)))
                    .append(" |\n");
            }
        }
        return out.toString();
    }

    /**
     * The arm of the A/B this run represents, used to name the report so both arms' outputs survive
     * side by side. Mirrors the cluster's {@code analytics.multivalue.surface_arrays}, which the test
     * JVM sees because the gradle task sets it on both.
     */
    private static String arm() {
        return Boolean.parseBoolean(System.getProperty("analytics.multivalue.surface_arrays", "true")) ? "arrays" : "scalar";
    }

    private static void write(String report) {
        write("multivalue-matrix-" + arm() + ".md", report);
    }

    /**
     * Writes the report next to the other build outputs. {@code tests.gradle.buildDir} is not set on
     * every runner, so fall back to the working directory rather than losing the report.
     */
    private static void write(String fileName, String content) {
        try {
            String buildDir = System.getProperty("tests.gradle.buildDir", "build");
            Path path = Paths.get(buildDir).resolve(fileName);
            Files.createDirectories(path.getParent());
            Files.writeString(path, content);
            logger.info("wrote {}", path.toAbsolutePath());
        } catch (IOException e) {
            logger.warn("could not write [{}]; the report is in the log above", fileName, e);
        }
    }

    private static String escapeCell(String s) {
        return s == null ? "" : s.replace("|", "\\|").replace("\n", " ");
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max) + "…";
    }
}
