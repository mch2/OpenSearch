/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class ArgMaxAggCallAdapterTests extends OpenSearchTestCase {

    private final ArgMaxAggCallAdapter adapter = new ArgMaxAggCallAdapter();
    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    public void testRewritesToLastValueWithAscNullsLastCollation() {
        // last_value(value) ORDER BY key ASC — the last row of an ascending sort is the
        // row with the largest key, giving us ARG_MAX semantics.
        AggregateCall original = argCall(SqlStdOperatorTable.ARG_MAX, /* valueIdx= */ 3, /* keyIdx= */ 7);

        AggregateCall adapted = adapter.apply(original);

        assertEquals(SqlStdOperatorTable.LAST_VALUE, adapted.getAggregation());
        assertEquals(List.of(3), adapted.getArgList());
        assertEquals(1, adapted.getCollation().getFieldCollations().size());
        RelFieldCollation sort = adapted.getCollation().getFieldCollations().get(0);
        assertEquals(7, sort.getFieldIndex());
        assertEquals(RelFieldCollation.Direction.ASCENDING, sort.getDirection());
        assertEquals(RelFieldCollation.NullDirection.LAST, sort.nullDirection);
    }

    public void testPreservesReturnTypeAndName() {
        AggregateCall original = argCall(SqlStdOperatorTable.ARG_MAX, 0, 1);

        AggregateCall adapted = adapter.apply(original);

        assertEquals(original.getType(), adapted.getType());
        assertEquals(original.getName(), adapted.getName());
    }

    public void testNonBinaryCallUnchanged() {
        AggregateCall unary = AggregateCall.create(
            SqlStdOperatorTable.ARG_MAX,
            false,
            false,
            false,
            List.of(0),
            -1,
            org.apache.calcite.rel.RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "arg_max_unary"
        );

        AggregateCall adapted = adapter.apply(unary);

        assertSame(unary, adapted);
    }

    private AggregateCall argCall(SqlAggFunction op, int valueIdx, int keyIdx) {
        return AggregateCall.create(
            op,
            false,
            false,
            false,
            List.of(valueIdx, keyIdx),
            -1,
            org.apache.calcite.rel.RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "arg_max_call"
        );
    }
}
