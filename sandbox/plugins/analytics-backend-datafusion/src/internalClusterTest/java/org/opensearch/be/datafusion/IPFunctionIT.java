/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end tests for PPL's IP comparison overloads against an {@code ip}-typed
 * field. PPL's {@code PPLFuncImpTable} registers {@code EQUALS_IP},
 * {@code LESS_IP}, etc. alongside the standard Calcite comparison operators; the
 * IP variant is selected when one operand has IP type, so the Substrait plan
 * carries e.g. {@code equals_ip(ip_field, '1.2.3.4')}. Verifies the full
 * pipeline: PPL, Calcite, Substrait, DataFusion, Rust UDF. The column arrives
 * as Binary (16-byte InetAddressPoint from parquet) and the literal as Utf8;
 * the UDF canonicalizes both to the 16-byte IPv4-mapped form.
 *
 * <p>The Calcite schema maps {@code ip} to {@code VARBINARY} so the Arrow
 * schema at scan time matches the parquet representation, and the sql repo's
 * {@code OpenSearchTypeFactory.convertSqlTypeNameToExprType} maps
 * {@code VARBINARY} back to {@code ExprType.IP} so PPL's overload resolution
 * picks the IP variant.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class IPFunctionIT extends OpenSearchIntegTestCase {

    private static final String IPS_INDEX = "ips";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPPLPlugin.class, FlightStreamPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (!indexExists(IPS_INDEX)) {
            createIpsIndex();
            indexIpsDocs();
            ensureGreen(IPS_INDEX);
            refresh(IPS_INDEX);
        }
    }

    public void testEqualsIp() {
        // Row id=1 stores 1.2.3.4; exactly one match.
        assertCount("source=" + IPS_INDEX + " | where ip_field = '1.2.3.4'", 1);
    }

    public void testNotEqualsIp() {
        // 3 total rows, 1 equals 1.2.3.4 → 2 don't.
        assertCount("source=" + IPS_INDEX + " | where ip_field != '1.2.3.4'", 2);
    }

    public void testLessIp() {
        // Under IPv4-mapped canonical form, plain IPv6 addresses sort greater
        // than any IPv4 address (0x20... > 0, 0, ..., 0xff, 0xff, ...). So
        // ip < 11.0.0.0 matches 1.2.3.4 and 10.0.0.1 only — 2001:db8::1 is
        // greater.
        assertCount("source=" + IPS_INDEX + " | where ip_field < '11.0.0.0'", 2);
    }

    public void testLessOrEqualIp() {
        // ip <= 1.2.3.4 matches only 1.2.3.4 itself under IPv4-mapped ordering.
        assertCount("source=" + IPS_INDEX + " | where ip_field <= '1.2.3.4'", 1);
    }

    public void testGreaterIp() {
        // ip > 1.2.3.4 matches 10.0.0.1 and 2001:db8::1 (IPv4-mapped rule).
        assertCount("source=" + IPS_INDEX + " | where ip_field > '1.2.3.4'", 2);
    }

    public void testGreaterOrEqualIp() {
        // ip >= 1.2.3.4 matches all three rows.
        assertCount("source=" + IPS_INDEX + " | where ip_field >= '1.2.3.4'", 3);
    }

    // ---- Fixture ----------------------------------------------------------

    private void createIpsIndex() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("id").field("type", "long").endObject()
            .startObject("ip_field").field("type", "ip").endObject()
            .endObject()
            .endObject();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin().indices().prepareCreate(IPS_INDEX).setSettings(indexSettings).setMapping(mapping).get();
        assertTrue("ips index creation must be acknowledged", response.isAcknowledged());
    }

    private void indexIpsDocs() {
        client().prepareIndex(IPS_INDEX).setId("1").setSource("id", 1, "ip_field", "1.2.3.4").get();
        client().prepareIndex(IPS_INDEX).setId("2").setSource("id", 2, "ip_field", "10.0.0.1").get();
        client().prepareIndex(IPS_INDEX).setId("3").setSource("id", 3, "ip_field", "2001:db8::1").get();
    }

    // ---- Assertion helper -------------------------------------------------

    private void assertCount(String ppl, int expectedRows) {
        PPLRequest request = new PPLRequest(ppl);
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, request).actionGet();
        assertNotNull("PPLResponse must not be null for " + ppl, response);
        assertEquals("row count for " + ppl, expectedRows, response.getRows().size());
    }
}
