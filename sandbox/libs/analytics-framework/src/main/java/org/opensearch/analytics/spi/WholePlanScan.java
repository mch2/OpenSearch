/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Per-scan metadata for whole-plan lowering (whole-plan-lowering-spec.md D12). One entry per
 * base-table scan in the whole query; carries the delegation tree shape and QTF row-id signal the
 * native scan-leaf swap needs. {@code delegated} predicates are added in Phase 3.
 *
 * @param table          the scanned index/table name (matches the Substrait NamedTable name)
 * @param treeShape      ordinal of the {@link FilterTreeShape} derived before annotation stripping
 * @param requestsRowIds whether the scan projects the QTF row-id column
 *
 * @opensearch.internal
 */
public record WholePlanScan(String table, int treeShape, boolean requestsRowIds) {}
