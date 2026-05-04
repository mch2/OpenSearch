/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableExpression;
import io.substrait.extension.ImmutableSimpleExtension;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.relation.Aggregate;
import io.substrait.relation.ExpressionCopyOnWriteVisitor;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.util.EmptyVisitationContext;

/**
 * Single-pass post-processor for Substrait plans before serialization to protobuf.
 *
 * <p>Applies two kinds of rewrites:
 * <ul>
 *   <li><b>Rel-level</b> — structural changes like table name stripping, handled by
 *       {@link RelCopyOnWriteVisitor} overrides.</li>
 *   <li><b>Expression-level</b> — literal/type fixes handled by
 *       {@link ExpressionCopyOnWriteVisitor} overrides. Adding a new expression rewrite
 *       only requires overriding the corresponding {@code visit} method.</li>
 * </ul>
 *
 * @opensearch.internal
 */
class SubstraitPlanRewriter {

    private SubstraitPlanRewriter() {}

    static Plan rewrite(Plan plan) {
        PlanRelVisitor visitor = new PlanRelVisitor();

        List<Plan.Root> roots = new ArrayList<>();
        for (Plan.Root root : plan.getRoots()) {
            Optional<Rel> modified = root.getInput().accept(visitor, null);
            roots.add(modified.isPresent() ? Plan.Root.builder().from(root).input(modified.get()).build() : root);
        }
        return Plan.builder().from(plan).roots(roots).build();
    }

    /**
     * Rel-level visitor. Handles structural rewrites (table name stripping). Expression
     * rewrites live in {@link PlanExpressionVisitor} and are wired via the superclass
     * constructor so the base walker uses them for every rel type (project, filter,
     * aggregate, …) automatically.
     */
    private static class PlanRelVisitor extends RelCopyOnWriteVisitor<RuntimeException> {

        private static final String CATALOG_PREFIX = "opensearch";

        PlanRelVisitor() {
            super(PlanExpressionVisitor::new);
        }

        // Strip "opensearch" catalog prefix: ["opensearch", "index_name"] -> ["index_name"]
        @Override
        public Optional<Rel> visit(NamedScan namedScan, EmptyVisitationContext ctx) {
            List<String> names = namedScan.getNames();
            if (names.size() > 1 && CATALOG_PREFIX.equals(names.get(0))) {
                return Optional.of(NamedScan.builder().from(namedScan).names(names.subList(1, names.size())).build());
            }
            return super.visit(namedScan, ctx);
        }

        // Inline Project-hoisted literals back into Aggregate measure args.
        //
        // Calcite's RelBuilder.AggCall.Registrar hoists any non-RexInputRef AggregateCall arg
        // into the child Project, so a PPL call like `percentile_approx(balance, 0.5)` arrives
        // at substrait as Aggregate(arg[0]=FieldReference(0), arg[1]=FieldReference(2)) with
        // Project(balance, gender, 0.5_literal). DataFusion's approx_percentile_cont validator
        // requires its fraction arg to be an Expr::Literal; a Column ref is rejected with
        //   "Percentile value for 'APPROX_PERCENTILE_CONT' must be a literal".
        //
        // Splice each FieldReference arg that points at a Project-defined literal back into
        // the aggregate inline. This is structure-preserving — if the Project later turns
        // out to have no other consumers, DataFusion's optimizer will prune it.
        //
        // Targeted narrowly at percentile-style declarations that DF is known to be strict
        // about. Other aggregates whose args are genuinely computed (sum over an expression,
        // etc.) are left untouched so we don't accidentally change semantics.
        @Override
        public Optional<Rel> visit(Aggregate aggregate, EmptyVisitationContext ctx) {
            // Splice BEFORE super.visit so the base walker sees (and preserves) our
            // rewritten measures. super.visit may return Optional.empty when nothing
            // changed downstream; if only our splice changed anything, propagate the
            // rewritten Aggregate upward explicitly.
            Aggregate target = spliceLiteralArgs(aggregate);
            Optional<Rel> recursed = super.visit(target, ctx);
            if (recursed.isPresent()) {
                return recursed;
            }
            return target == aggregate ? Optional.empty() : Optional.of(target);
        }

        /** Names (in substrait extension-catalog form) whose arg[1] must be an inline literal. */
        private static final java.util.Set<String> LITERAL_FRACTION_AGGREGATES = java.util.Set.of(
            "approx_percentile_cont",
            "percentile_approx"
        );

        /** Returns a rewritten {@link Aggregate} if any measure's args were spliced, otherwise
         *  the original instance. */
        private static Aggregate spliceLiteralArgs(Aggregate aggregate) {
            if (!(aggregate.getInput() instanceof Project project)) {
                return aggregate;
            }
            // Substrait Project output space: by default it is the concatenation of
            // [input fields (0..inputFieldCount-1) ++ project expressions]. Isthmus (and
            // PlanProtoConverter on the receiver side) emits a Rel.Remap when the Project
            // is Calcite-origin: the remap exposes ONLY the project expressions in the
            // outward-facing schema, starting at output index 0. In that case the
            // aggregate's FieldReference(K) resolves directly to projectExprs[K], NOT to
            // projectExprs[K - inputFieldCount].
            Optional<Rel.Remap> remap = project.getRemap();
            int projectBase;
            if (remap.isPresent()) {
                // With remap, projectExprs occupy the outward-facing output slots directly.
                projectBase = 0;
            } else {
                // Without remap, Calcite input cols are still visible in the output —
                // the expressions sit after them.
                projectBase = project.getInput().getRecordType().fields().size();
            }
            List<Expression> projectExprs = project.getExpressions();

            List<Aggregate.Measure> oldMeasures = aggregate.getMeasures();
            List<Aggregate.Measure> newMeasures = null;
            for (int i = 0; i < oldMeasures.size(); i++) {
                Aggregate.Measure m = oldMeasures.get(i);
                AggregateFunctionInvocation afi = m.getFunction();
                String fnName = afi.declaration().name();
                if (!LITERAL_FRACTION_AGGREGATES.contains(fnName)) continue;
                if (afi.arguments().size() < 2) continue;
                FunctionArg arg1 = afi.arguments().get(1);
                if (!(arg1 instanceof FieldReference fr)) continue;
                Optional<Expression> literal = resolveProjectLiteral(fr, projectBase, projectExprs);
                if (literal.isEmpty()) continue;

                List<FunctionArg> newArgs = new ArrayList<>(afi.arguments());
                newArgs.set(1, literal.get());
                AggregateFunctionInvocation newAfi = AggregateFunctionInvocation.builder()
                    .from(afi)
                    .arguments(newArgs)
                    .build();
                Aggregate.Measure newMeasure = Aggregate.Measure.builder()
                    .from(m)
                    .function(newAfi)
                    .build();
                if (newMeasures == null) newMeasures = new ArrayList<>(oldMeasures);
                newMeasures.set(i, newMeasure);
            }
            if (newMeasures == null) return aggregate;
            return Aggregate.builder().from(aggregate).measures(newMeasures).build();
        }

        /** Resolves a top-level root-struct FieldReference to the Project-defined expression
         *  it points at, returning it only if that expression is a literal. Returns empty for
         *  references to pass-through input columns, nested references, or non-literal
         *  projection expressions. */
        private static Optional<Expression> resolveProjectLiteral(
            FieldReference fr,
            int projectBase,
            List<Expression> projectExprs
        ) {
            if (fr.segments().size() != 1) return Optional.empty();
            if (!(fr.segments().get(0) instanceof FieldReference.StructField sf)) return Optional.empty();
            int idx = sf.offset();
            int projIdx = idx - projectBase;
            if (projIdx < 0 || projIdx >= projectExprs.size()) return Optional.empty();
            Expression expr = projectExprs.get(projIdx);
            if (expr instanceof Expression.Literal) {
                return Optional.of(expr);
            }
            return Optional.empty();
        }
    }

    /**
     * Expression-level visitor. Override a {@code visit} method to add a new rewrite.
     * The base class handles recursion into function arguments, casts, if-then, etc.
     */
    private static class PlanExpressionVisitor extends ExpressionCopyOnWriteVisitor<RuntimeException> {

        PlanExpressionVisitor(RelCopyOnWriteVisitor<RuntimeException> relVisitor) {
            super(relVisitor);
        }

        // Isthmus hardcodes timestamp literals to precision 6 (microseconds).
        // Parquet stores Timestamp(MILLISECOND), so convert to precision 3.
        @Override
        public Optional<Expression> visit(Expression.PrecisionTimestampLiteral pts, EmptyVisitationContext ctx) {
            if (pts.precision() != 3) {
                return Optional.of(
                    ImmutableExpression.PrecisionTimestampLiteral.builder()
                        .value(toMillis(pts.value(), pts.precision()))
                        .precision(3)
                        .nullable(pts.nullable())
                        .build()
                );
            }
            return Optional.empty();
        }

        // DataFusion's substrait consumer (v44+) doesn't implement the VarChar literal
        // variant — only plain String. Calcite's constant folding of functions whose
        // return type is a parameterized VARCHAR(N) (e.g. JSON_ARRAY/JSON_OBJECT fold to
        // VARCHAR(2000)) flows through substrait-isthmus's LiteralConverter, which maps
        // VARCHAR(N) with precision != UNSPECIFIED to VarCharLiteral. Downgrade those
        // literals to StrLiteral here so DataFusion accepts the plan. The wire value
        // (the string bytes) is identical; only the declared length parameter is dropped.
        @Override
        public Optional<Expression> visit(Expression.VarCharLiteral vc, EmptyVisitationContext ctx) {
            return Optional.of(
                ImmutableExpression.StrLiteral.builder()
                    .value(vc.value())
                    .nullable(vc.nullable())
                    .build()
            );
        }

        // DataFusion's physical planner rejects `count(<anything>) OVER (…)`; it only
        // accepts the 0-arg form. By the time we see this expression, DataFusionFragment-
        // Convertor has rewritten `COUNT(col) OVER` → `COUNT() OVER` at the Calcite level,
        // but isthmus's WindowFunctionConverter emits the resulting RexOver with a synthetic
        // `1` literal operand (Calcite's RexBuilder.makeOver canonicalizes empty-operand
        // COUNT through the window signatures). Strip the args here so the emitted plan
        // uses the 0-arg variant DataFusion accepts.
        @Override
        public Optional<Expression> visit(Expression.WindowFunctionInvocation wfi, EmptyVisitationContext ctx) {
            Optional<Expression> recursed = super.visit(wfi, ctx);
            Expression.WindowFunctionInvocation current =
                (Expression.WindowFunctionInvocation) recursed.orElse(wfi);
            if (!"count".equals(current.declaration().name()) || current.arguments().isEmpty()) {
                return recursed;
            }
            SimpleExtension.WindowFunctionVariant zeroArg = ImmutableSimpleExtension.WindowFunctionVariant.builder()
                .from(current.declaration())
                .args(List.of())
                .build();
            return Optional.of(
                Expression.WindowFunctionInvocation.builder()
                    .from(current)
                    .declaration(zeroArg)
                    .arguments(List.of())
                    .build()
            );
        }
    }

    private static long toMillis(long value, int precision) {
        return switch (precision) {
            case 0 -> value * 1000L;
            case 6 -> TimeUnit.MICROSECONDS.toMillis(value);
            case 9 -> TimeUnit.NANOSECONDS.toMillis(value);
            default -> throw new IllegalArgumentException(
                "Unsupported timestamp precision: " + precision + ". Expected 0 (seconds), 6 (micros), or 9 (nanos)."
            );
        };
    }
}
