/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Coordinator-side row reshape produced by {@link ObjectFieldStitch}. Each output column is
 * either a {@link Output.Passthrough} of a single engine column, or an {@link Output.ObjectMap}
 * that gathers a subset of engine columns into a nested {@code Map<String,Object>}.
 *
 * <p>Pure runtime: no Calcite dependency. Identifies engine columns by integer index only.
 *
 * @opensearch.internal
 */
public record Stitch(List<Output> outputs) {

    /** Apply the stitch to engine rows; returns one user-visible row per engine row. */
    public List<Object[]> apply(Iterable<Object[]> engineRows) {
        List<Object[]> out = new ArrayList<>();
        for (Object[] row : engineRows) {
            Object[] outRow = new Object[outputs.size()];
            for (int i = 0; i < outputs.size(); i++) {
                outRow[i] = outputs.get(i).read(row);
            }
            out.add(outRow);
        }
        return out;
    }

    /** Output column names, in declaration order. */
    public List<String> names() {
        List<String> names = new ArrayList<>(outputs.size());
        for (Output col : outputs) names.add(col.name());
        return names;
    }

    /** A single user-visible output column. */
    public sealed interface Output permits Output.Passthrough, Output.ObjectMap {

        String name();

        /** Read this output's value from one engine row. */
        Object read(Object[] engineRow);

        /** Forward one engine column unchanged. */
        record Passthrough(String name, int engineColumnIndex) implements Output {
            @Override
            public Object read(Object[] engineRow) {
                return engineRow[engineColumnIndex];
            }
        }

        /** Build a nested Map from engine columns according to a recursive child structure. */
        record ObjectMap(String name, Map<String, MapSource> children) implements Output {
            @Override
            public Object read(Object[] engineRow) {
                return MapSource.buildMap(children, engineRow);
            }
        }
    }

    /** A child of an {@link Output.ObjectMap}: either a leaf engine column or a nested map. */
    public sealed interface MapSource permits MapSource.Leaf, MapSource.Nested {

        Object read(Object[] engineRow);

        record Leaf(int engineColumnIndex) implements MapSource {
            @Override
            public Object read(Object[] engineRow) {
                return engineRow[engineColumnIndex];
            }
        }

        record Nested(Map<String, MapSource> children) implements MapSource {
            @Override
            public Object read(Object[] engineRow) {
                return buildMap(children, engineRow);
            }
        }

        static Map<String, Object> buildMap(Map<String, MapSource> children, Object[] engineRow) {
            Map<String, Object> result = new LinkedHashMap<>(children.size());
            for (Map.Entry<String, MapSource> e : children.entrySet()) {
                result.put(e.getKey(), e.getValue().read(engineRow));
            }
            return result;
        }
    }
}
