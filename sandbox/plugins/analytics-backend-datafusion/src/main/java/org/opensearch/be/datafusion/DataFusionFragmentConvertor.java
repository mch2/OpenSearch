/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

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
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Optionality;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.ArrayList;
import java.util.List;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.NameBasedAggregateFunctionConverter;
import io.substrait.isthmus.expression.NameBasedScalarFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Filter;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Sort;

/**
 * Converts Calcite RelNode fragments to Substrait protobuf bytes
 * for the DataFusion Rust runtime.
 *
 * <p>Dispatch summary:
 * <ul>
 *   <li>{@link #convertShardScanFragment(String, RelNode)} and
 *       {@link #convertFinalAggFragment(RelNode)} — full-fragment conversions via
 *       {@link #convertToSubstrait(RelNode)}.</li>
 *   <li>{@link #attachPartialAggOnTop(RelNode, byte[])} and
 *       {@link #attachFragmentOnTop(RelNode, byte[])} — convert the wrapping
 *       operator standalone, then rewire its input to the decoded inner plan's
 *       root via {@link #rewire(Plan, Rel)}.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class DataFusionFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(DataFusionFragmentConvertor.class);

    private final SimpleExtension.ExtensionCollection extensions;

    public DataFusionFragmentConvertor(SimpleExtension.ExtensionCollection extensions) {
        this.extensions = extensions;
    }

    @Override
    public byte[] convertShardScanFragment(String tableName, RelNode fragment) {
        LOGGER.debug("Converting shard scan fragment for table [{}]", tableName);
        return convertToSubstrait(fragment);
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        LOGGER.debug("Attaching partial aggregate on top of {} inner bytes", innerBytes.length);
        Plan inner = decodePlan(innerBytes);
        Rel wrapper = convertStandalone(partialAggFragment);
        Plan rewired = rewire(inner, withAggregationPhase(wrapper, Expression.AggregationPhase.INITIAL_TO_INTERMEDIATE));
        return serializePlan(rewired);
    }

    @Override
    public byte[] convertFinalAggFragment(RelNode fragment) {
        LOGGER.debug("Converting final-aggregate fragment");
        RelNode rewritten = rewriteStageInputScans(fragment);
        return convertToSubstrait(rewritten);
    }

    @Override
    public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        LOGGER.debug("Attaching generic fragment [{}] on top of {} inner bytes", fragment.getClass().getSimpleName(), innerBytes.length);
        Plan inner = decodePlan(innerBytes);
        Rel wrapper = convertStandalone(fragment);
        return serializePlan(rewire(inner, wrapper));
    }

    // ── Core conversion helpers ─────────────────────────────────────────────────

    private byte[] convertToSubstrait(RelNode fragment) {
        RelRoot root = RelRoot.of(fragment, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(fragment);
        Rel substraitRel = visitor.apply(root.rel);

        List<String> fieldNames = root.fields.stream().map(field -> field.getValue()).toList();

        Plan.Root substraitRoot = Plan.Root.builder().input(substraitRel).names(fieldNames).build();
        Plan plan = Plan.builder().addRoots(substraitRoot).build();

        plan = SubstraitPlanRewriter.rewrite(plan);

        io.substrait.proto.Plan protoPlan = new PlanProtoConverter().toProto(plan);
        byte[] bytes = protoPlan.toByteArray();
        LOGGER.debug("Substrait plan: {} bytes", bytes.length);
        return bytes;
    }

    private Rel convertStandalone(RelNode operator) {
        SubstraitRelVisitor visitor = createVisitor(operator);
        return visitor.apply(operator);
    }

    static Plan rewire(Plan inner, Rel wrapper) {
        if (inner.getRoots().isEmpty()) {
            throw new IllegalArgumentException("Inner Substrait plan has no root relation to rewire under wrapper");
        }
        Plan.Root innerRoot = inner.getRoots().get(0);
        Rel innerRel = innerRoot.getInput();
        Rel rewired = replaceInput(wrapper, innerRel);
        return Plan.builder().addRoots(Plan.Root.builder().input(rewired).names(innerRoot.getNames()).build()).build();
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
            return Project.builder().from(project).input(newInput).build();
        }
        throw new UnsupportedOperationException(
            "Cannot attach-on-top a Substrait Rel of type " + wrapper.getClass().getSimpleName() + " — no single-input rewire defined"
        );
    }

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

    private static RelNode rewriteStageInputScans(RelNode node) {
        if (node instanceof OpenSearchStageInputScan scan) {
            return new StageInputTableScan(scan.getCluster(), scan.getTraitSet(), DatafusionReduceSink.INPUT_ID, scan.getRowType());
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

        List<FunctionMappings.Sig> additionalAggSigs = List.of(FunctionMappings.s(stubAgg("take"), "take"));
        AggregateFunctionConverter aggConverter = new NameBasedAggregateFunctionConverter(
            extensions.aggregateFunctions(),
            additionalAggSigs,
            typeFactory,
            typeConverter
        );
        ScalarFunctionConverter scalarConverter = new NameBasedScalarFunctionConverter(
            extensions.scalarFunctions(),
            List.of(),
            typeFactory,
            typeConverter
        );
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(extensions.windowFunctions(), typeFactory);

        ConverterProvider provider = new ConverterProvider(
            typeFactory,
            extensions,
            scalarConverter,
            aggConverter,
            windowConverter,
            typeConverter
        );
        return new SubstraitRelVisitor(provider);
    }

    private static SqlAggFunction stubAgg(String name) {
        return new SqlAggFunction(
            name,
            null,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0,
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            false,
            false,
            Optionality.FORBIDDEN
        ) {};
    }

    // ── Plan serde helpers ──────────────────────────────────────────────────────

    private Plan decodePlan(byte[] bytes) {
        try {
            io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(bytes);
            return new ProtoPlanConverter(extensions).from(proto);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to decode Substrait plan bytes", e);
        }
    }

    private static byte[] serializePlan(Plan plan) {
        return new PlanProtoConverter().toProto(plan).toByteArray();
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
