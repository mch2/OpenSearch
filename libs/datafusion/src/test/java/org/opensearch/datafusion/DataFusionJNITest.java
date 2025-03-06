/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

//import io.substrait.isthmus.SqlToSubstrait;
//import io.substrait.proto.Plan;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.search.stream.collector.DataFusionAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.notNullValue;
import static org.opensearch.search.stream.collector.PushStreamingCollector.COUNT;
import static org.opensearch.search.stream.collector.PushStreamingCollector.ORD;

public class DataFusionJNITest extends OpenSearchTestCase {

    static {
        // Load the JNI library
        System.loadLibrary("datafusion_jni");
    }

    @Test
    public void testSerDe() throws Exception {
        for (int j = 0; j < 1000; j++) {
        BufferAllocator allocator = AccessController.doPrivileged((PrivilegedAction<BufferAllocator>) () -> new RootAllocator(Integer.MAX_VALUE));
        FieldType fieldType = new FieldType(true, new ArrowType.Int(64, false), null);
        Map<String, Field> arrowFields = new HashMap<>();
        Field arrowField = new Field("category", fieldType, null);
        arrowFields.put("category", arrowField);

        Schema schema = new Schema(new ArrayList<>(arrowFields.values()));
        VectorSchemaRoot r = VectorSchemaRoot.create(schema, allocator);
        UInt8Vector category = (UInt8Vector) r.getVector("category");

        for (int i = 0; i < 10; i++) {
            category.setSafe(i, 1);
        }

        r.setRowCount(10);

        try (DataFusionAggregator aggregator = new DataFusionAggregator("category")) {
            DataFrame fut = aggregator.exportBatch(
                allocator,
                r
            ).join();
            RecordBatchStream recordBatchStream = fut.getStream(allocator).join();
            VectorSchemaRoot aggregatedRoot = recordBatchStream.getVectorSchemaRoot();
            while (recordBatchStream.loadNextBatch().join()) {
                UInt8Vector dfVector = (UInt8Vector) aggregatedRoot.getVector(ORD);
                BigIntVector cv = (BigIntVector)  aggregatedRoot.getVector(COUNT);
                for (int i = 0; i < dfVector.getValueCount(); i++) {
                    long l = dfVector.get(i);
                    long c = cv.get(i);
                    logger.info("ROW {} COUNT {}", l, c);
                }
            }
         }
//        allocator.close();
        }
    }

    @Test
    public void testExecuteSubstrait() throws Exception {
//        String sql = "SELECT * from ord";
//        SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
//        Plan plan = sqlToSubstrait.execute(sql, List.of("tableName"));
//        // Serialize the plan
//        // Execute the plan
//        DataFrame df = DataFusion.executeSubstrait(plan.toByteArray()).get();
//
//        // Verify we got a non-null DataFrame back
//        assertThat(df, notNullValue());
//        df.close();
    }
}
