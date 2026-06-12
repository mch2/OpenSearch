/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.AnnotationResolver;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.DelegationPossibleFunction;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Annotation stripping + intra-operator delegation-byte accumulation, extracted
 * verbatim from {@code FragmentConversionDriver} (df-proto migration §5, file
 * pointers). This is a <b>move, not a rewrite</b>: the delegation resolver's
 * behavior is on the spec's DO-NOT-TOUCH list, so the logic here is byte-for-byte
 * the original. {@code StageConversionDriver} (the new per-stage proto path) and
 * the legacy {@code FragmentConversionDriver} both call into this class so there
 * is exactly one implementation of the strip + resolver contract.
 *
 * @opensearch.internal
 */
public final class AnnotationStripper {

    private static final Logger LOGGER = LogManager.getLogger(AnnotationStripper.class);

    private AnnotationStripper() {}

    /** Recursively strips annotations bottom-up. Keeps OpenSearchStageInputScan as-is. */
    public static RelNode strip(RelNode node, IntraOperatorDelegationBytes delegationBytes) {
        if (node instanceof OpenSearchStageInputScan) {
            return node; // kept for schema inference at reduce stage
        }
        if (node instanceof OpenSearchExchangeReducer) {
            return strip(node.getInputs().getFirst(), delegationBytes);
        }
        List<RelNode> strippedChildren = new ArrayList<>(node.getInputs().size());
        for (RelNode input : node.getInputs()) {
            strippedChildren.add(strip(input, delegationBytes));
        }
        if (node instanceof OpenSearchRelNode openSearchNode) {
            Function<OperatorAnnotation, RexNode> resolver = delegationBytes.resolverFor(openSearchNode, node.getCluster().getRexBuilder());
            if (node instanceof OpenSearchFilter filter && resolver instanceof AnnotationResolver ar) {
                // Combine delegated predicates in a single pass, then strip with simple unwrapper
                RexNode resolved = ar.resolveTree(filter.getCondition());
                RexNode flattened = RexUtil.flatten(node.getCluster().getRexBuilder(), resolved);
                return LogicalFilter.create(strippedChildren.getFirst(), flattened);
            }
            return openSearchNode.stripAnnotations(strippedChildren, resolver);
        }
        boolean childrenChanged = false;
        for (int i = 0; i < strippedChildren.size(); i++) {
            if (strippedChildren.get(i) != node.getInputs().get(i)) {
                childrenChanged = true;
                break;
            }
        }
        return childrenChanged ? node.copy(node.getTraitSet(), strippedChildren) : node;
    }

    /**
     * Accumulates serialized delegated query bytes during fragment conversion.
     *
     * <p>The resolver performs a single bottom-up traversal of the filter condition tree,
     * classifying each node as delegated (targets a non-operator backend like Lucene) or
     * native (evaluated by the driving backend). Tree-walking and combining logic is
     * delegated to {@link DelegatedPredicateCombiner}.
     */
    public static final class IntraOperatorDelegationBytes {
        private final CapabilityRegistry registry;
        private List<DelegatedExpression> delegatedExpressions;

        public IntraOperatorDelegationBytes(CapabilityRegistry registry) {
            this.registry = registry;
        }

        /**
         * Creates an annotation resolver that does a single bottom-up traversal.
         * Maximal same-backend delegated subtrees are converted via the backend's
         * {@code DelegatedSubtreeConvertor} into one DelegatedExpression each.
         */
        Function<OperatorAnnotation, RexNode> resolverFor(OpenSearchRelNode operator, RexBuilder rexBuilder) {
            String operatorBackend = operator.getViableBackends().getFirst();
            List<FieldStorageInfo> fieldStorage = operator.getOutputFieldStorage();
            if (delegatedExpressions == null) delegatedExpressions = new ArrayList<>();
            DelegatedPredicateCombiner classifier = new DelegatedPredicateCombiner(
                operatorBackend,
                fieldStorage,
                registry,
                rexBuilder,
                delegatedExpressions
            );
            return new AnnotationResolver() {

                @Override
                public RexNode resolveTree(RexNode condition) {
                    DelegatedPredicateCombiner.Classified result = classifier.classify(condition, this::apply);
                    if (result instanceof DelegatedPredicateCombiner.Delegated d) {
                        return classifier.finalizeDelegated(d);
                    }
                    return ((DelegatedPredicateCombiner.Resolved) result).node();
                }

                @Override
                public RexNode apply(OperatorAnnotation annotation) {
                    String annotationBackend = annotation.getViableBackends().getFirst();
                    if (annotationBackend.equals(operatorBackend)) {
                        // Performance-delegation candidate: dual-viable predicate kept on the operator's backend,
                        // but a peer can be opportunistically consulted at runtime.
                        if (annotation instanceof AnnotatedPredicate ap && !ap.getPerformanceDelegationBackends().isEmpty()) {
                            String peerBackend = ap.getPerformanceDelegationBackends().getFirst();
                            RexNode original = ap.unwrap();
                            if (!(original instanceof RexCall originalCall)) {
                                throw new IllegalStateException("Performance-delegation candidate must wrap a RexCall: " + original);
                            }
                            ScalarFunction function = ScalarFunction.fromSqlOperatorWithFallback(originalCall.getOperator());
                            DelegatedPredicateSerializer serializer = registry.getBackend(peerBackend)
                                .delegatedPredicateSerializers()
                                .get(function);
                            if (serializer == null) {
                                LOGGER.debug(
                                    "Performance-delegation skipped: no serializer for [{}] on delegated backend [{}]; falling back to native on operator [{}]",
                                    function,
                                    peerBackend,
                                    operatorBackend
                                );
                                return annotation.unwrap();
                            }
                            byte[] serialized = serializer.serialize(originalCall, fieldStorage);
                            LOGGER.debug(
                                "Performance-delegated annotation [id={}]: {} kept on operator [{}], wrapped for peer [{}], serialized {} bytes",
                                ap.getAnnotationId(),
                                function,
                                operatorBackend,
                                peerBackend,
                                serialized.length
                            );
                            if (delegatedExpressions == null) {
                                delegatedExpressions = new ArrayList<>();
                            }
                            delegatedExpressions.add(new DelegatedExpression(ap.getAnnotationId(), peerBackend, serialized));
                            return DelegationPossibleFunction.makeCall(rexBuilder, originalCall, ap.getAnnotationId());
                        }
                        LOGGER.debug(
                            "Native annotation [id={}]: backend [{}] matches operator",
                            annotation.getAnnotationId(),
                            operatorBackend
                        );
                        return annotation.unwrap();
                    }
                    RexNode original = annotation.unwrap();
                    if (!(original instanceof RexCall originalCall) || !(originalCall.getOperator() instanceof SqlFunction sqlFunction)) {
                        throw new IllegalStateException("Delegated expression must be a SqlFunction call: " + original);
                    }
                    ScalarFunction function = ScalarFunction.fromSqlFunction(sqlFunction);
                    DelegatedPredicateSerializer serializer = registry.getBackend(annotationBackend)
                        .getCapabilityProvider()
                        .delegatedPredicateSerializers()
                        .get(function);
                    if (serializer == null) {
                        throw new IllegalStateException(
                            "No DelegatedPredicateSerializer for ["
                                + function
                                + "] on backend ["
                                + annotationBackend
                                + "]. CapabilityRegistry should have rejected this at startup."
                        );
                    }
                    byte[] serialized = serializer.serialize(originalCall, fieldStorage);
                    LOGGER.debug(
                        "Delegated annotation [id={}]: {} from operator [{}] to [{}], serialized {} bytes",
                        annotation.getAnnotationId(),
                        function,
                        operatorBackend,
                        annotationBackend,
                        serialized.length
                    );
                    if (delegatedExpressions == null) {
                        delegatedExpressions = new ArrayList<>();
                    }
                    delegatedExpressions.add(new DelegatedExpression(annotation.getAnnotationId(), annotationBackend, serialized));
                    return annotation.makePlaceholder(rexBuilder);
                }
            };
        }

        public List<DelegatedExpression> getResult() {
            return delegatedExpressions != null ? delegatedExpressions : List.of();
        }
    }
}
