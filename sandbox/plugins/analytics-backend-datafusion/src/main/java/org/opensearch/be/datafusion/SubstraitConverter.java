/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.Rel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.delegation.DelegationBroker;
import org.opensearch.analytics.plan.operators.OpenSearchHybridFilter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handles all Substrait conversion concerns for the DataFusion backend:
 * <ul>
 *   <li>Calcite RelNode → Substrait bytes</li>
 *   <li>Hybrid filter rewriting (strip delegated predicates)</li>
 *   <li>Delegation metadata embedding via AdvancedExtension</li>
 *   <li>Table name extraction from Substrait bytes</li>
 *   <li>Schema prefix stripping from NamedTable references</li>
 * </ul>
 */
final class SubstraitConverter {

    private static final Logger logger = LogManager.getLogger(SubstraitConverter.class);

    private static volatile SimpleExtension.ExtensionCollection EXTENSIONS;

    private SubstraitConverter() {}

    // ---- Conversion ----

    /**
     * Converts a Calcite RelNode to serialized Substrait plan bytes.
     */
    static byte[] convert(RelNode fragment) {
        RelRoot root = RelRoot.of(fragment, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(fragment);
        Rel substraitRel = visitor.apply(root.rel);

        List<String> fieldNames = root.fields.stream()
            .map(f -> f.getValue())
            .collect(Collectors.toList());

        Plan plan = Plan.builder()
            .addRoots(Plan.Root.builder().input(substraitRel).names(fieldNames).build())
            .build();

        io.substrait.proto.Plan protoPlan = new PlanProtoConverter().toProto(plan);
        return stripSchemaFromPlan(protoPlan);
    }

    // ---- Hybrid filter rewriting ----

    /**
     * Rewrites the plan tree, replacing {@link OpenSearchHybridFilter} nodes with
     * plain {@link LogicalFilter} nodes containing only the primary backend's predicates.
     * Secondary backend predicates are handled via delegation callback.
     */
    static RelNode rewriteHybridFilters(RelNode node) {
        List<RelNode> newInputs = new ArrayList<>();
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = rewriteHybridFilters(input);
            newInputs.add(rewritten);
            if (rewritten != input) changed = true;
        }
        RelNode current = changed ? node.copy(node.getTraitSet(), newInputs) : node;

        if (current instanceof OpenSearchHybridFilter) {
            OpenSearchHybridFilter hybrid = (OpenSearchHybridFilter) current;
            RexNode primaryPredicate = hybrid.getBackendPredicates().get(hybrid.getBackendTag());
            RexNode condition = primaryPredicate != null ? primaryPredicate : hybrid.getCondition();
            return LogicalFilter.create(hybrid.getInput(), condition);
        }
        return current;
    }

    // ---- Delegation embedding ----

    /**
     * Embeds delegation metadata into a Substrait plan as an {@code AdvancedExtension}.
     * The Rust side reads this to know when to call back to Java via
     * {@link DelegationBroker#delegateFilter}.
     *
     * @param substraitBytes      the serialized Substrait plan
     * @param delegationContextId the broker-assigned context ID
     * @param segMaxDocs          per-segment max doc counts, or null
     * @param targetBackend       the delegation target backend name
     * @return the plan with delegation metadata embedded
     */
    static byte[] embedDelegation(byte[] substraitBytes,
            long delegationContextId, long[] segMaxDocs, String targetBackend) {
        try {
            io.substrait.proto.Plan plan = io.substrait.proto.Plan.parseFrom(substraitBytes);

            StringBuilder json = new StringBuilder();
            json.append("{\"delegationContextId\":").append(delegationContextId);
            if (segMaxDocs != null) {
                json.append(",\"segMaxDocs\":[");
                for (int i = 0; i < segMaxDocs.length; i++) {
                    if (i > 0) json.append(",");
                    json.append(segMaxDocs[i]);
                }
                json.append("]");
            }
            json.append(",\"target\":\"").append(targetBackend).append("\"}");

            logger.info("[SubstraitConverter] Embedding delegation metadata: {}", json);

            com.google.protobuf.Any delegationAny = com.google.protobuf.Any.newBuilder()
                .setTypeUrl("opensearch/delegation")
                .setValue(com.google.protobuf.ByteString.copyFromUtf8(json.toString()))
                .build();

            io.substrait.proto.AdvancedExtension advExt =
                io.substrait.proto.AdvancedExtension.newBuilder()
                    .addOptimization(delegationAny)
                    .build();

            return plan.toBuilder()
                .setAdvancedExtensions(advExt)
                .build()
                .toByteArray();
        } catch (Exception e) {
            logger.error("Failed to embed delegation metadata", e);
            return substraitBytes;
        }
    }

    // ---- Table name extraction ----

    /**
     * Extracts the table name from serialized Substrait plan bytes.
     */
    static String extractTableName(byte[] substraitBytes) {
        try {
            io.substrait.proto.Plan plan = io.substrait.proto.Plan.parseFrom(substraitBytes);
            for (io.substrait.proto.PlanRel rel : plan.getRelationsList()) {
                if (rel.hasRoot()) {
                    String name = findTableName(rel.getRoot().getInput());
                    if (name != null) return name;
                }
            }
        } catch (Exception e) {
            // fall through
        }
        return "hits"; // fallback
    }

    private static String findTableName(io.substrait.proto.Rel rel) {
        if (rel.hasRead() && rel.getRead().hasNamedTable()) {
            var names = rel.getRead().getNamedTable().getNamesList();
            return names.isEmpty() ? null : names.get(names.size() - 1);
        }
        if (rel.hasFilter()) return findTableName(rel.getFilter().getInput());
        if (rel.hasProject()) return findTableName(rel.getProject().getInput());
        if (rel.hasAggregate()) return findTableName(rel.getAggregate().getInput());
        if (rel.hasSort()) return findTableName(rel.getSort().getInput());
        if (rel.hasFetch()) return findTableName(rel.getFetch().getInput());
        return null;
    }

    // ---- Schema stripping ----

    private static byte[] stripSchemaFromPlan(io.substrait.proto.Plan plan) {
        io.substrait.proto.Plan.Builder builder = plan.toBuilder();
        for (int i = 0; i < builder.getRelationsCount(); i++) {
            io.substrait.proto.PlanRel rel = builder.getRelations(i);
            if (rel.hasRoot()) {
                io.substrait.proto.RelRoot root = rel.getRoot();
                io.substrait.proto.Rel fixed = stripSchemaFromRel(root.getInput());
                builder.setRelations(i, rel.toBuilder().setRoot(root.toBuilder().setInput(fixed)).build());
            }
        }
        return builder.build().toByteArray();
    }

    private static io.substrait.proto.Rel stripSchemaFromRel(io.substrait.proto.Rel rel) {
        io.substrait.proto.Rel.Builder b = rel.toBuilder();
        if (rel.hasRead() && rel.getRead().hasNamedTable()) {
            io.substrait.proto.ReadRel read = rel.getRead();
            io.substrait.proto.ReadRel.NamedTable table = read.getNamedTable();
            if (table.getNamesCount() > 1) {
                String bareName = table.getNames(table.getNamesCount() - 1);
                b.setRead(read.toBuilder().setNamedTable(table.toBuilder().clearNames().addNames(bareName)));
            }
        }
        if (rel.hasFilter())
            b.setFilter(rel.getFilter().toBuilder().setInput(stripSchemaFromRel(rel.getFilter().getInput())));
        if (rel.hasProject())
            b.setProject(rel.getProject().toBuilder().setInput(stripSchemaFromRel(rel.getProject().getInput())));
        if (rel.hasAggregate())
            b.setAggregate(rel.getAggregate().toBuilder().setInput(stripSchemaFromRel(rel.getAggregate().getInput())));
        if (rel.hasSort())
            b.setSort(rel.getSort().toBuilder().setInput(stripSchemaFromRel(rel.getSort().getInput())));
        if (rel.hasFetch())
            b.setFetch(rel.getFetch().toBuilder().setInput(stripSchemaFromRel(rel.getFetch().getInput())));
        return b.build();
    }

    // ---- Substrait visitor setup ----

    private static SimpleExtension.ExtensionCollection getExtensions() {
        if (EXTENSIONS == null) {
            synchronized (SubstraitConverter.class) {
                if (EXTENSIONS == null) {
                    Thread t = Thread.currentThread();
                    ClassLoader original = t.getContextClassLoader();
                    t.setContextClassLoader(SubstraitConverter.class.getClassLoader());
                    try {
                        EXTENSIONS = DefaultExtensionCatalog.DEFAULT_COLLECTION;
                    } finally {
                        t.setContextClassLoader(original);
                    }
                }
            }
        }
        return EXTENSIONS;
    }

    private static SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        TypeConverter typeConverter = TypeConverter.DEFAULT;

        List<FunctionMappings.Sig> aggSigs = List.of(
            new FunctionMappings.Sig(SqlStdOperatorTable.COUNT, "count"),
            new FunctionMappings.Sig(SqlStdOperatorTable.SUM, "sum"),
            new FunctionMappings.Sig(SqlStdOperatorTable.SUM0, "sum0"),
            new FunctionMappings.Sig(SqlStdOperatorTable.MIN, "min"),
            new FunctionMappings.Sig(SqlStdOperatorTable.MAX, "max"),
            new FunctionMappings.Sig(SqlStdOperatorTable.AVG, "avg"),
            new FunctionMappings.Sig(SqlStdOperatorTable.STDDEV, "std_dev"),
            new FunctionMappings.Sig(SqlStdOperatorTable.STDDEV_POP, "std_dev"),
            new FunctionMappings.Sig(SqlStdOperatorTable.STDDEV_SAMP, "std_dev"),
            new FunctionMappings.Sig(SqlStdOperatorTable.VARIANCE, "variance"),
            new FunctionMappings.Sig(SqlStdOperatorTable.VAR_POP, "variance"),
            new FunctionMappings.Sig(SqlStdOperatorTable.VAR_SAMP, "variance")
        );

        return new SubstraitRelVisitor(
            typeFactory,
            new ScalarFunctionConverter(getExtensions().scalarFunctions(), Collections.emptyList(), typeFactory, typeConverter),
            new AggregateFunctionConverter(getExtensions().aggregateFunctions(), aggSigs, typeFactory, typeConverter),
            new WindowFunctionConverter(getExtensions().windowFunctions(), typeFactory),
            typeConverter,
            ImmutableFeatureBoard.builder().build()
        );
    }
}
