/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableAggregateFunctionInvocation;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Sort;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.proto.TypeProtoConverter;

/** Converts Calcite RelNode fragments to Substrait protobuf bytes for the DataFusion Rust runtime. */
public class DataFusionFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(DataFusionFragmentConvertor.class);

    /**
     * Shared {@link TypeProtoConverter} for schema-only conversions. Safe as a singleton
     * because schema-only Reads convert primitive Calcite types to primitive Substrait
     * protos — no functions or user-defined types touch the inner {@link ExtensionCollector},
     * so it never accumulates per-call state. Avoids re-allocating both objects on every
     * {@link #convertSchemaOnlyRead} call.
     */
    private static final TypeProtoConverter SCHEMA_ONLY_TYPE_PROTO_CONVERTER = new TypeProtoConverter(new ExtensionCollector());

    private final SimpleExtension.ExtensionCollection extensions;

    public DataFusionFragmentConvertor(SimpleExtension.ExtensionCollection extensions) {
        this.extensions = extensions;
    }

    @Override
    public byte[] convertFragment(RelNode fragment) {
        LOGGER.debug("Converting fragment [{}]", fragment.getClass().getSimpleName());
        RelNode rewritten = rewriteStageInputScans(fragment);
        return convertToSubstrait(rewritten);
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        LOGGER.debug("Attaching partial aggregate on top of {} inner bytes", innerBytes.length);
        Plan inner = decodePlan(innerBytes);
        Rel wrapper = convertStandalone(partialAggFragment);
        Plan rewired = rewire(
            inner,
            withAggregationPhase(wrapper, Expression.AggregationPhase.INITIAL_TO_INTERMEDIATE),
            fieldNames(partialAggFragment)
        );
        return serializePlan(SubstraitPlanPojoRewriter.rewrite(rewired));
    }

    /**
     * Builds a schema-only stub plan directly via Substrait protos — no isthmus, no
     * Calcite RelNode round-trip. Output:
     * <pre>
     *   Plan { relations: [PlanRel { Root { input: Rel { Read { named_table: "input-&lt;id&gt;";
     *                                                          base_schema: rowType } },
     *                                 names: rowType.fieldNames }}] }
     * </pre>
     *
     * <p>Used by the LM stage path: LM runs Java-only scatter/gather/stitch and emits no
     * Substrait compute, but the parent reduce sink (Stage 3) still calls
     * {@code registerPartitionStream} which needs the partition's named-table id and base
     * schema. This stub is the minimum proto that satisfies that path. Bypassing isthmus
     * avoids unnecessary {@code SubstraitRelVisitor} setup and keeps the produced bytes
     * tightly scoped to the schema we care about.
     */
    @Override
    public byte[] convertSchemaOnlyRead(int childStageId, RelDataType rowType) {
        // Fully-qualified names below: io.substrait.proto.{Plan,Rel,NamedStruct,RelRoot} clash with already-imported single-name imports.
        NamedStruct ns = TypeConverter.DEFAULT.toNamedStruct(rowType);
        io.substrait.proto.NamedStruct nsProto = ns.toProto(SCHEMA_ONLY_TYPE_PROTO_CONVERTER);

        ReadRel readRel = ReadRel.newBuilder()
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames("input-" + childStageId).build())
            .setBaseSchema(nsProto)
            .build();

        io.substrait.proto.Rel inputRel = io.substrait.proto.Rel.newBuilder().setRead(readRel).build();
        PlanRel planRel = PlanRel.newBuilder()
            .setRoot(io.substrait.proto.RelRoot.newBuilder().setInput(inputRel).addAllNames(rowType.getFieldNames()).build())
            .build();

        byte[] bytes = SubstraitPlanProtoRewriter.rewrite(io.substrait.proto.Plan.newBuilder().addRelations(planRel).build()).toByteArray();
        LOGGER.debug("Schema-only Read for stage [{}]: {} bytes", childStageId, bytes.length);
        return bytes;
    }

    @Override
    public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        LOGGER.debug("Attaching generic fragment [{}] on top of {} inner bytes", fragment.getClass().getSimpleName(), innerBytes.length);
        Plan inner = decodePlan(innerBytes);
        RelNode rewritten = rewriteStageInputScans(fragment);
        Rel wrapper = convertStandalone(rewritten);
        // Rewriter must run on the assembled plan so wrapper literals get rewritten alongside the inner.
        return serializePlan(SubstraitPlanPojoRewriter.rewrite(rewire(inner, wrapper, fieldNames(fragment))));
    }

    private byte[] convertToSubstrait(RelNode fragment) {
        // TODO: move rewriters that don't touch substrait-specific classes up to the analytics-engine
        // layer so other backends can reuse them.
        RelNode preprocessed = UntypedNullPreprocessor.rewrite(fragment);
        preprocessed = PplAggregateCallRewriter.rewrite(preprocessed);
        preprocessed = PplWindowCallRewriter.rewrite(preprocessed);
        preprocessed = ItemTypeRebuilder.rewrite(preprocessed);
        preprocessed = CastToVarcharRewriter.rewrite(preprocessed);
        preprocessed = CastTemporalLiteralValidator.rewrite(preprocessed);
        RelRoot root = RelRoot.of(preprocessed, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(preprocessed);
        Rel substraitRel;
        try {
            substraitRel = visitor.apply(root.rel);
        } catch (AssertionError e) {
            // Substrait validators throw AssertionError directly (not via `assert`), so -da
            // doesn't gate them; convert to a normal exception so we don't crash the cluster.
            throw new IllegalStateException("Substrait conversion rejected the plan: " + e.getMessage(), e);
        }

        List<String> fieldNames = root.fields.stream().map(field -> field.getValue()).toList();

        Plan.Root substraitRoot = Plan.Root.builder().input(substraitRel).names(fieldNames).build();
        Plan plan = Plan.builder().addRoots(substraitRoot).build();

        plan = SubstraitPlanPojoRewriter.rewrite(plan);

        io.substrait.proto.Plan protoPlan = SubstraitPlanProtoRewriter.rewrite(new PlanProtoConverter().toProto(plan));
        byte[] bytes = protoPlan.toByteArray();
        LOGGER.debug("Substrait plan: {} bytes", bytes.length);
        return bytes;
    }

    /** Converts a single operator into a Substrait {@link Rel}; children are discarded and rewired by {@link #rewire}. */
    private Rel convertStandalone(RelNode operator) {
        RelNode preprocessed = UntypedNullPreprocessor.rewrite(operator);
        preprocessed = PplAggregateCallRewriter.rewrite(preprocessed);
        preprocessed = PplWindowCallRewriter.rewrite(preprocessed);
        preprocessed = ItemTypeRebuilder.rewrite(preprocessed);
        preprocessed = CastToVarcharRewriter.rewrite(preprocessed);
        preprocessed = CastTemporalLiteralValidator.rewrite(preprocessed);
        SubstraitRelVisitor visitor = createVisitor(preprocessed);
        return visitor.apply(preprocessed);
    }

    /** Rewires {@code wrapper} above {@code inner}'s root; {@code wrapperNames} must match the wrapper's output schema. */
    static Plan rewire(Plan inner, Rel wrapper, List<String> wrapperNames) {
        if (inner.getRoots().isEmpty()) {
            throw new IllegalArgumentException("Inner Substrait plan has no root relation to rewire under wrapper");
        }
        Plan.Root innerRoot = inner.getRoots().get(0);
        Rel innerRel = innerRoot.getInput();
        Rel rewired = replaceInput(wrapper, innerRel);
        return Plan.builder().addRoots(Plan.Root.builder().input(rewired).names(wrapperNames).build()).build();
    }

    /** Wrapper's output column names from its Calcite row type. */
    private static List<String> fieldNames(RelNode fragment) {
        return fragment.getRowType().getFieldList().stream().map(RelDataTypeField::getName).toList();
    }

    private static Rel replaceInput(Rel wrapper, Rel newInput) {
        if (wrapper instanceof Aggregate agg) {
            return Aggregate.builder().from(agg).input(newInput).build();
        }
        if (wrapper instanceof Sort sort) {
            return Sort.builder().from(sort).input(newInput).build();
        }
        if (wrapper instanceof Filter filter) {
            return Filter.builder().from(filter).input(newInput).build();
        }
        if (wrapper instanceof Project project) {
            // Lifted-window shape: outer Project references a window column from the lower Project.
            if (project.getInput() instanceof Project lower && containsWindowFunction(lower)) {
                Rel rewiredLower = replaceInput(lower, newInput);
                return Project.builder().from(project).input(rewiredLower).build();
            }
            return Project.builder().from(project).input(newInput).build();
        }
        if (wrapper instanceof Fetch fetch) {
            // A single Calcite LogicalSort carrying both a collation AND a fetch/offset lowers to
            // Fetch(Sort(input)) — two Substrait rels from one node. Rewiring the Fetch's input
            // directly would drop the Sort and lose global order before the limit. Descend into
            // the Sort so the shape becomes Fetch(Sort(newInput)): gather, sort globally, then limit.
            Rel rewiredInput = fetch.getInput() instanceof Sort ? replaceInput(fetch.getInput(), newInput) : newInput;
            return Fetch.builder().from(fetch).input(rewiredInput).build();
        }
        throw new UnsupportedOperationException(
            "Cannot attach-on-top a Substrait Rel of type " + wrapper.getClass().getSimpleName() + " — no single-input rewire defined"
        );
    }

    private static boolean containsWindowFunction(Project project) {
        for (Expression expr : project.getExpressions()) {
            if (expr instanceof Expression.WindowFunctionInvocation) {
                return true;
            }
        }
        return false;
    }

    /** Forces {@code phase} on every measure of an Aggregate wrapper (isthmus hardcodes INITIAL_TO_RESULT). */
    private static Rel withAggregationPhase(Rel rel, Expression.AggregationPhase phase) {
        if (!(rel instanceof Aggregate agg)) {
            return rel;
        }
        List<Aggregate.Measure> newMeasures = new ArrayList<>(agg.getMeasures().size());
        for (Aggregate.Measure m : agg.getMeasures()) {
            AggregateFunctionInvocation fn = m.getFunction();
            AggregateFunctionInvocation rephased = AggregateFunctionInvocation.builder().from(fn).aggregationPhase(phase).build();
            newMeasures.add(Aggregate.Measure.builder().from(m).function(rephased).build());
        }
        return Aggregate.builder().from(agg).measures(newMeasures).build();
    }

    /** Rewrites {@link OpenSearchStageInputScan} leaves to TableScan with {@code "input-<childStageId>"} names. */
    private static RelNode rewriteStageInputScans(RelNode node) {
        if (node instanceof OpenSearchStageInputScan scan) {
            return new StageInputTableScan(scan.getCluster(), scan.getTraitSet(), "input-" + scan.getChildStageId(), scan.getRowType());
        }
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = rewriteStageInputScans(input);
            newInputs.add(rewritten);
            if (rewritten != input) {
                changed = true;
            }
        }
        if (changed) {
            return node.copy(node.getTraitSet(), newInputs);
        }
        return node;
    }

    // ── Visitor wiring ──────────────────────────────────────────────────────────

    private SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        TypeConverter typeConverter = TypeConverter.DEFAULT;
        ScalarFunctionConverter scalarConverter = new ScalarFunctionConverter(
            extensions.scalarFunctions(),
            LocalFunctionOps.ADDITIONAL_SCALAR_SIGS,
            typeFactory,
            typeConverter
        );
        // Filter isthmus's default APPROX_COUNT_DISTINCT binding so our `approx_distinct` entry wins.
        // The convert() override inlines literal-Project columns into the AggregateFunctionInvocation
        // as Substrait literals so two-stage UDAFs (e.g. TAKE's N) see the constant on the Final side.
        AggregateFunctionConverter aggConverter = new AggregateFunctionConverter(
            extensions.aggregateFunctions(),
            LocalFunctionOps.ADDITIONAL_AGGREGATE_SIGS,
            typeFactory,
            typeConverter
        ) {
            @Override
            protected ImmutableList<FunctionMappings.Sig> getSigs() {
                return super.getSigs().stream()
                    .filter(sig -> sig.operator != SqlStdOperatorTable.APPROX_COUNT_DISTINCT)
                    .collect(ImmutableList.toImmutableList());
            }

            @Override
            public Optional<AggregateFunctionInvocation> convert(
                RelNode input,
                Type.Struct inputType,
                AggregateCall call,
                Function<RexNode, Expression> rexConverter
            ) {
                Optional<AggregateFunctionInvocation> bound = super.convert(input, inputType, call, rexConverter);
                if (bound.isEmpty()) {
                    return bound;
                }
                // Let the op rewrite its data args (e.g. a type-coercing CAST) on the bound Substrait
                // argument — generic dispatch; the cast semantics live on the LocalAggOp, not here.
                Optional<AggregateFunctionInvocation> rewrittenArgs = rewriteLocalAggDataArgs(input, call, bound.get(), rexConverter);
                if (rewrittenArgs.isPresent()) {
                    return rewrittenArgs;
                }
                if (!(input instanceof org.apache.calcite.rel.core.Project project)) {
                    return bound;
                }
                AggregateFunctionInvocation fn = bound.get();
                List<RexNode> projects = project.getProjects();
                List<FunctionArg> args = fn.arguments();
                List<FunctionArg> rewritten = null;
                RexBuilder rexBuilder = project.getCluster().getRexBuilder();
                for (int i = 0; i < args.size(); i++) {
                    FunctionArg arg = args.get(i);
                    if (!(arg instanceof io.substrait.expression.FieldReference fr)) continue;
                    Integer offset = simpleStructOffset(fr);
                    if (offset == null || offset < 0 || offset >= projects.size()) continue;
                    if (!(projects.get(offset) instanceof RexLiteral rexLit)) continue;
                    if (rewritten == null) rewritten = new ArrayList<>(args);
                    RexNode toConvert = call.getAggregation() instanceof LocalFunctionOps.LocalAggOp localOp
                        ? localOp.normaliseLiteralArg(i, rexLit, rexBuilder, typeFactory)
                        : rexLit;
                    rewritten.set(i, rexConverter.apply(toConvert));
                }
                if (rewritten == null) return bound;
                return Optional.of(ImmutableAggregateFunctionInvocation.builder().from(fn).arguments(rewritten).build());
            }
        };
        // Same APPROX_COUNT_DISTINCT filter as aggConverter — let our `approx_distinct` entry win.
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(
            extensions.windowFunctions(),
            LocalFunctionOps.ADDITIONAL_WINDOW_SIGS,
            typeFactory,
            typeConverter
        ) {
            @Override
            protected ImmutableList<FunctionMappings.Sig> getSigs() {
                return super.getSigs().stream()
                    .filter(sig -> sig.operator != SqlStdOperatorTable.APPROX_COUNT_DISTINCT)
                    .collect(ImmutableList.toImmutableList());
            }
        };
        ConverterProvider converterProvider = new ConverterProvider(
            typeFactory,
            extensions,
            scalarConverter,
            aggConverter,
            windowConverter,
            typeConverter
        );
        return new SubstraitRelVisitor(converterProvider) {
            @Override
            public Rel visit(org.apache.calcite.rel.core.Aggregate aggregate) {
                Rel rel = super.visit(aggregate);
                return rel instanceof Aggregate agg ? addNullArgFilters(aggregate, agg) : rel;
            }
        };
    }

    /**
     * Adds an {@code is_not_null} {@code preMeasureFilter} to each measure whose {@link LocalFunctionOps.LocalAggOp}
     * declares {@link LocalFunctionOps.LocalAggOp#filtersNullArgs} — so the converter stays generic and only the op
     * opts in (DataFusion's substrait consumer can't take the function's own {@code ignore_nulls}).
     * Measures line up with the Calcite agg calls minus any {@code GROUP_ID()} (which isthmus drops).
     */
    private Aggregate addNullArgFilters(org.apache.calcite.rel.core.Aggregate calcite, Aggregate agg) {
        List<AggregateCall> calls = calcite.getAggCallList()
            .stream()
            .filter(c -> c.getAggregation() != SqlStdOperatorTable.GROUP_ID)
            .toList();
        List<Aggregate.Measure> measures = agg.getMeasures();
        if (calls.size() != measures.size()) {
            return agg; // shape we don't recognise — leave untouched
        }
        List<Aggregate.Measure> rewritten = null;
        for (int i = 0; i < measures.size(); i++) {
            Aggregate.Measure m = measures.get(i);
            if (!(calls.get(i).getAggregation() instanceof LocalFunctionOps.LocalAggOp op) || !op.filtersNullArgs(calls.get(i))) {
                continue;
            }
            if (m.getPreMeasureFilter().isPresent()
                || m.getFunction().arguments().isEmpty()
                || !(m.getFunction().arguments().get(0) instanceof Expression argExpr)) {
                continue;
            }
            Expression filter = isNotNull(argExpr);
            if (filter == null) {
                continue;
            }
            if (rewritten == null) {
                rewritten = new ArrayList<>(measures);
            }
            rewritten.set(i, Aggregate.Measure.builder().from(m).preMeasureFilter(filter).build());
        }
        return rewritten == null ? agg : Aggregate.builder().from(agg).measures(rewritten).build();
    }

    /** Builds {@code is_not_null(arg)} from the merged extension catalog, or null if the variant is absent. */
    private Expression isNotNull(Expression arg) {
        SimpleExtension.ScalarFunctionVariant variant = extensions.scalarFunctions()
            .stream()
            .filter(f -> "is_not_null".equals(f.name()))
            .findFirst()
            .orElse(null);
        if (variant == null) {
            return null;
        }
        return io.substrait.expression.ImmutableExpression.ScalarFunctionInvocation.builder()
            .declaration(variant)
            .addArguments(arg)
            .outputType(io.substrait.type.TypeCreator.REQUIRED.BOOLEAN)
            .build();
    }

    /** Column offset for a simple input-rooted single-segment {@code StructField}, else null. */
    private static Integer simpleStructOffset(io.substrait.expression.FieldReference fr) {
        if (fr.isOuterReference() || fr.isLambdaParameterReference()) return null;
        if (!fr.inputExpression().isEmpty()) return null;
        if (fr.segments().size() != 1) return null;
        io.substrait.expression.FieldReference.ReferenceSegment seg = fr.segments().get(0);
        if (!(seg instanceof io.substrait.expression.FieldReference.StructField sf)) return null;
        return sf.offset();
    }

    /**
     * Lets a {@link LocalFunctionOps.LocalAggOp} rewrite its data args on the bound Substrait invocation (e.g. a
     * type-coercing CAST), keyed only on the generic hook — no per-function logic here. Returns
     * empty when the op is not a {@code LocalAggOp} or leaves every arg unchanged.
     */
    private Optional<AggregateFunctionInvocation> rewriteLocalAggDataArgs(
        RelNode input,
        AggregateCall call,
        AggregateFunctionInvocation fn,
        Function<RexNode, Expression> rexConverter
    ) {
        if (!(call.getAggregation() instanceof LocalFunctionOps.LocalAggOp op)) {
            return Optional.empty();
        }
        RexBuilder rexBuilder = input.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = input.getCluster().getTypeFactory();
        List<FunctionArg> rewritten = null;
        for (int i = 0; i < call.getArgList().size(); i++) {
            RelDataType srcType = input.getRowType().getFieldList().get(call.getArgList().get(i)).getType();
            RexNode argRef = rexBuilder.makeInputRef(srcType, call.getArgList().get(i));
            Optional<RexNode> replacement = op.rewriteDataArg(i, argRef, rexBuilder, typeFactory);
            if (replacement.isEmpty()) {
                continue;
            }
            if (rewritten == null) {
                rewritten = new ArrayList<>(fn.arguments());
            }
            rewritten.set(i, rexConverter.apply(replacement.get()));
        }
        List<FunctionArg> args = rewritten != null ? rewritten : fn.arguments();
        // Sort the elements ascending by the (rewritten) first arg when the op asks for it — emitted
        // as the invocation's sort, which DataFusion's array_agg honours (its DISTINCT+ORDER BY rule
        // is satisfied because the sort key IS the argument expression).
        List<Expression.SortField> sorts = fn.sort();
        boolean addedSort = false;
        if (op.sortsArgAscending(call) && sorts.isEmpty() && !args.isEmpty() && args.get(0) instanceof Expression sortKey) {
            sorts = List.of(
                io.substrait.expression.ImmutableExpression.SortField.builder()
                    .expr(sortKey)
                    .direction(Expression.SortDirection.ASC_NULLS_LAST)
                    .build()
            );
            addedSort = true;
        }
        if (rewritten == null && !addedSort) {
            return Optional.empty();
        }
        return Optional.of(ImmutableAggregateFunctionInvocation.builder().from(fn).arguments(args).sort(sorts).build());
    }

    // ── Plan serde helpers ──────────────────────────────────────────────────────

    /** Decodes serialized Substrait bytes into a model-level {@link Plan}. */
    private Plan decodePlan(byte[] bytes) {
        try {
            io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(bytes);
            return new ProtoPlanConverter(extensions).from(proto);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to decode Substrait plan bytes", e);
        }
    }

    /** Serializes a model-level {@link Plan} to proto bytes. */
    private static byte[] serializePlan(Plan plan) {
        return SubstraitPlanProtoRewriter.rewrite(new PlanProtoConverter().toProto(plan)).toByteArray();
    }

    // ── Calcite TableScan wrappers for OpenSearchStageInputScan rewrite ─────────

    static final class StageInputTableScan extends TableScan {
        StageInputTableScan(RelOptCluster cluster, RelTraitSet traitSet, String stageInputId, RelDataType rowType) {
            super(cluster, traitSet, List.of(), new StageInputRelOptTable(stageInputId, rowType));
        }
    }

    static final class StageInputRelOptTable implements RelOptTable {
        private final List<String> qualifiedName;
        private final RelDataType rowType;

        StageInputRelOptTable(String stageInputId, RelDataType rowType) {
            this.qualifiedName = List.of(stageInputId);
            this.rowType = rowType;
        }

        @Override
        public List<String> getQualifiedName() {
            return qualifiedName;
        }

        @Override
        public RelDataType getRowType() {
            return rowType;
        }

        @Override
        public double getRowCount() {
            return 100;
        }

        @Override
        public RelOptSchema getRelOptSchema() {
            return null;
        }

        @Override
        public RelNode toRel(ToRelContext context) {
            throw new UnsupportedOperationException("StageInputRelOptTable.toRel not supported");
        }

        @Override
        public List<ColumnStrategy> getColumnStrategies() {
            return List.of();
        }

        @Override
        public <C> C unwrap(Class<C> aClass) {
            return null;
        }

        @Override
        public boolean isKey(ImmutableBitSet columns) {
            return false;
        }

        @Override
        public List<ImmutableBitSet> getKeys() {
            return List.of();
        }

        @Override
        public List<RelReferentialConstraint> getReferentialConstraints() {
            return List.of();
        }

        @Override
        public List<RelCollation> getCollationList() {
            return List.of();
        }

        @Override
        public RelDistribution getDistribution() {
            return RelDistributions.ANY;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public org.apache.calcite.linq4j.tree.Expression getExpression(Class clazz) {
            return null;
        }

        @Override
        public RelOptTable extend(List<RelDataTypeField> extendedFields) {
            return this;
        }
    }
}
