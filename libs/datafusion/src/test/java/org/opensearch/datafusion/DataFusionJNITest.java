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
import org.junit.Test;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.notNullValue;

public class DataFusionJNITest extends OpenSearchTestCase {

    static {
        // Load the JNI library
        System.loadLibrary("datafusion_jni");
    }

//    @Test
//    public void testSerDe() throws ExecutionException, InterruptedException {
////        CompletableFuture<DataFrame> dataFrameCompletableFuture = DataFusion.aggWithEndpoints(List.of(new StreamManager.Endpoint("http://localhost:9200", new byte[]{1, 2, 3, 4})));
////        dataFrameCompletableFuture.get();
//        CompletableFuture<String> myString = DataFusion.runAsync("MY STRING");
//        myString.whenComplete((s, throwable) -> {
//            System.out.println(s);
//        });
//        System.out.println("RUNNING SOMETHING ASYNC");
//        myString.get();
//    }

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
