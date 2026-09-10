/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa.multivalue;

import java.util.List;
import java.util.Set;

/**
 * One query shape in the multi-value matrix, plus the set of OpenSearch types it applies to.
 *
 * <p>{@code template} contains {@code {f}} for the field under test plus the literal placeholders
 * {@code {lit1}}, {@code {lit2}} and {@code {likePattern}}, filled per type from {@link Literals}.
 * The same template is run against the scalar column and the multi-value column of every applicable
 * type, and the two outcomes are compared — see {@code MultiValueFunctionMatrixIT}.
 */
public record Probe(String id, Category category, String template, Set<String> types) {

    /**
     * Per-type literals for the comparison probes. {@code lit1} is the value the first element of
     * every document holds, so {@code {f} = {lit1}} matches every non-empty document under
     * "contains" semantics and only the single-valued ones under "whole value equals" semantics —
     * the difference the matrix is looking for. {@code lit2} is the second element's value.
     */
    public record Literals(String lit1, String lit2, String likePattern) {
        public static Literals forType(String osType) {
            return switch (osType) {
                case "keyword", "text" -> new Literals("'alpha'", "'beta'", "'al%'");
                case "long" -> new Literals("10", "20", "'1%'");
                case "integer" -> new Literals("1", "2", "'1%'");
                case "double" -> new Literals("1.5", "2.5", "'1%'");
                case "boolean" -> new Literals("true", "false", "'t%'");
                case "date" -> new Literals("'2026-01-01 00:00:00'", "'2026-06-15 12:30:00'", "'2026%'");
                case "ip" -> new Literals("'10.0.0.1'", "'192.168.1.1'", "'10.%'");
                default -> throw new IllegalArgumentException("no literals for type [" + osType + "]");
            };
        }
    }

    /** Groups probes in the report; also the axis along which a rewrite strategy is decided. */
    public enum Category {
        PROJECT,
        FILTER_EQUALITY,
        FILTER_RANGE,
        FILTER_NULL,
        FILTER_TEXT,
        SORT,
        GROUP_BY,
        AGGREGATE,
        AGGREGATE_COLLECT,
        SCALAR_STRING,
        SCALAR_NUMERIC,
        SCALAR_DATE,
        SCALAR_IP,
        ARRAY_NATIVE,
        COMMAND,
        JOIN
    }

    public static final Set<String> STRING = Set.of("keyword", "text");
    public static final Set<String> NUMERIC = Set.of("long", "integer", "double");
    public static final Set<String> DATE = Set.of("date");
    public static final Set<String> IP = Set.of("ip");
    public static final Set<String> BOOLEAN = Set.of("boolean");
    public static final Set<String> ALL = Set.of("keyword", "text", "long", "integer", "double", "boolean", "date", "ip");
    /** Types whose values compare and order meaningfully — excludes boolean, where ranges are noise. */
    public static final Set<String> ORDERED = Set.of("keyword", "text", "long", "integer", "double", "date", "ip");

    /** Renders the probe against one field of one index, filling the per-type literals. */
    public String render(String index, String field, Literals literals) {
        return "source="
            + index
            + " "
            + template.replace("{f}", field)
                .replace("{lit1}", literals.lit1())
                .replace("{lit2}", literals.lit2())
                .replace("{likePattern}", literals.likePattern());
    }

    public boolean appliesTo(String osType) {
        return types.contains(osType);
    }

    /**
     * The matrix. Ordered by category so the report reads as a walk from "can we read the column at
     * all" through filters, ordering, grouping, aggregation, scalar functions, and commands.
     *
     * <p>Every probe is a shape a user can type today against a single-valued field. A probe that
     * fails only on the multi-value column is a gap Approach A has to close, either with a planner
     * rewrite or with a kernel.
     */
    public static List<Probe> all() {
        return List.of(
            // ── projection ──────────────────────────────────────────────────────────────────────
            new Probe("project", Category.PROJECT, "| fields id, {f} | sort id", ALL),
            new Probe("project-head", Category.PROJECT, "| head 3 | fields {f}", ALL),
            // Late materialization: sort+head above the anchor makes the planner fetch {f} by row id
            // in a second phase, which is a different schema path than the streaming scan.
            new Probe("project-late-materialize", Category.PROJECT, "| sort id | head 3 | fields {f}", ALL),

            // ── equality / membership filters ───────────────────────────────────────────────────
            // The core Approach A question: on a multi-valued field, OpenSearch's DSL semantics for
            // `= v` is "any element equals v" (contains), not "the value equals v".
            new Probe("filter-eq", Category.FILTER_EQUALITY, "| where {f} = {lit1} | stats count()", ALL),
            new Probe("filter-neq", Category.FILTER_EQUALITY, "| where {f} != {lit1} | stats count()", ALL),
            new Probe("filter-in", Category.FILTER_EQUALITY, "| where {f} in ({lit1}, {lit2}) | stats count()", ALL),

            // ── range filters ───────────────────────────────────────────────────────────────────
            new Probe("filter-gt", Category.FILTER_RANGE, "| where {f} > {lit1} | stats count()", ORDERED),
            new Probe("filter-between", Category.FILTER_RANGE, "| where {f} >= {lit1} and {f} <= {lit2} | stats count()", ORDERED),

            // ── null / existence ────────────────────────────────────────────────────────────────
            // Also the empty-array question: is `[]` null, or present-and-empty?
            new Probe("filter-isnull", Category.FILTER_NULL, "| where isnull({f}) | stats count()", ALL),
            new Probe("filter-isnotnull", Category.FILTER_NULL, "| where isnotnull({f}) | stats count()", ALL),

            // ── text / pattern filters ──────────────────────────────────────────────────────────
            new Probe("filter-like", Category.FILTER_TEXT, "| where like({f}, {likePattern}) | stats count()", STRING),
            new Probe("filter-match", Category.FILTER_TEXT, "| where match({f}, {lit1}) | stats count()", STRING),

            // ── ordering ────────────────────────────────────────────────────────────────────────
            new Probe("sort-asc", Category.SORT, "| sort {f} | fields id, {f}", ORDERED),
            new Probe("sort-desc", Category.SORT, "| sort - {f} | fields id, {f}", ORDERED),

            // ── grouping ────────────────────────────────────────────────────────────────────────
            // A terms aggregation on a multi-valued keyword field puts a document in one bucket per
            // element. A row-oriented GROUP BY over a LIST column keys on the whole array. These are
            // different answers to the same query; the probe shows which one comes back.
            new Probe("group-by", Category.GROUP_BY, "| stats count() by {f} | sort {f}", ALL),
            new Probe("group-by-two-metrics", Category.GROUP_BY, "| stats count(), count(id) by {f} | sort {f}", ALL),
            new Probe("top", Category.GROUP_BY, "| top 3 {f}", ALL),
            new Probe("rare", Category.GROUP_BY, "| rare 3 {f}", ALL),
            new Probe("eventstats-by", Category.GROUP_BY, "| eventstats count() as c by {f} | fields id, c | sort id", ALL),
            new Probe("dedup", Category.GROUP_BY, "| dedup {f} | stats count()", ALL),

            // ── aggregates over the column ──────────────────────────────────────────────────────
            new Probe("agg-count", Category.AGGREGATE, "| stats count({f})", ALL),
            new Probe("agg-distinct-count", Category.AGGREGATE, "| stats distinct_count({f})", ALL),
            new Probe("agg-min-max", Category.AGGREGATE, "| stats min({f}), max({f})", ORDERED),
            new Probe("agg-sum-avg", Category.AGGREGATE, "| stats sum({f}), avg({f})", NUMERIC),
            new Probe("agg-percentile", Category.AGGREGATE, "| stats percentile({f}, 50)", NUMERIC),

            // ── collecting aggregates (already array-typed on the way out) ──────────────────────
            new Probe("agg-values", Category.AGGREGATE_COLLECT, "| stats values({f})", ALL),
            new Probe("agg-list", Category.AGGREGATE_COLLECT, "| stats list({f})", ALL),

            // ── scalar functions: string ────────────────────────────────────────────────────────
            new Probe("scalar-upper", Category.SCALAR_STRING, "| eval x = upper({f}) | fields id, x | sort id", STRING),
            new Probe("scalar-length", Category.SCALAR_STRING, "| eval x = length({f}) | fields id, x | sort id", STRING),
            new Probe("scalar-substr", Category.SCALAR_STRING, "| eval x = substr({f}, 1, 2) | fields id, x | sort id", STRING),
            new Probe("scalar-concat", Category.SCALAR_STRING, "| eval x = concat({f}, '!') | fields id, x | sort id", STRING),
            new Probe("scalar-trim", Category.SCALAR_STRING, "| eval x = trim({f}) | fields id, x | sort id", STRING),

            // ── scalar functions: numeric ───────────────────────────────────────────────────────
            new Probe("scalar-abs", Category.SCALAR_NUMERIC, "| eval x = abs({f}) | fields id, x | sort id", NUMERIC),
            new Probe("scalar-plus", Category.SCALAR_NUMERIC, "| eval x = {f} + 1 | fields id, x | sort id", NUMERIC),
            new Probe("scalar-round", Category.SCALAR_NUMERIC, "| eval x = round({f}) | fields id, x | sort id", NUMERIC),
            new Probe("scalar-cast-string", Category.SCALAR_NUMERIC, "| eval x = cast({f} as string) | fields id, x | sort id", NUMERIC),

            // ── scalar functions: date ──────────────────────────────────────────────────────────
            new Probe("scalar-year", Category.SCALAR_DATE, "| eval x = year({f}) | fields id, x | sort id", DATE),
            new Probe("scalar-date-format", Category.SCALAR_DATE, "| eval x = date_format({f}, '%Y-%m') | fields id, x | sort id", DATE),
            new Probe("scalar-span", Category.SCALAR_DATE, "| stats count() by span({f}, 1d)", DATE),

            // ── scalar functions: ip ────────────────────────────────────────────────────────────
            new Probe("scalar-cidrmatch", Category.SCALAR_IP, "| where cidrmatch({f}, '10.0.0.0/8') | stats count()", IP),

            // ── array-native functions (the upside of surfacing ARRAY) ──────────────────────────
            // These are meaningless on a scalar column and expected to fail there; on the multi
            // column they are the natural way to express element-level intent.
            new Probe("array-length", Category.ARRAY_NATIVE, "| eval x = array_length({f}) | fields id, x | sort id", ALL),
            new Probe("array-mvjoin", Category.ARRAY_NATIVE, "| eval x = mvjoin({f}, ',') | fields id, x | sort id", STRING),
            new Probe("array-mvdedup", Category.ARRAY_NATIVE, "| eval x = mvdedup({f}) | fields id, x | sort id", ALL),
            new Probe("array-mvindex", Category.ARRAY_NATIVE, "| eval x = mvindex({f}, 0) | fields id, x | sort id", ALL),

            // ── commands that reshape rows around the column ────────────────────────────────────
            new Probe("command-rename", Category.COMMAND, "| rename {f} as renamed | fields id, renamed | sort id", ALL),
            new Probe("command-stats-then-filter", Category.COMMAND, "| stats count() as c by {f} | where c > 1 | sort {f}", ALL),
            new Probe("command-sort-head-fields", Category.COMMAND, "| sort {f} | head 2 | fields {f}", ORDERED),

            // ── self join on the column ─────────────────────────────────────────────────────────
            // Join keys go through hash partitioning, so a LIST key exercises the row encoder and
            // the shuffle path rather than just a kernel.
            new Probe("join-on-field", Category.JOIN, "| join left=l right=r on l.{f} = r.{f} " + MultiValueDataset.INDEX + " | stats count()", ALL)
        );
    }
}
