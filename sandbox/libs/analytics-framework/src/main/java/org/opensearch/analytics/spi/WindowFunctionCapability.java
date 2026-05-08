/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Set;

/**
 * Declares that a backend can evaluate a specific {@link WindowFunction} on a specific
 * {@link FieldType} in the given data formats.
 *
 * <p>Mirrors {@link AggregateCapability} in shape. Field-type set is ignored for
 * window functions whose result is independent of input type (e.g. {@code ROW_NUMBER()});
 * backends declare the full supported set for consistency with the other capability
 * records.
 *
 * @opensearch.internal
 */
public record WindowFunctionCapability(WindowFunction function, Set<FieldType> fieldTypes, Set<String> formats) {
}
