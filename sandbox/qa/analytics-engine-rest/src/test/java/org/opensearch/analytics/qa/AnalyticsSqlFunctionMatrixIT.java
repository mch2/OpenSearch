/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Live correctness smoke matrix (R4) for the SQL front-end. Off-cluster lowering tests (the unit
 * fail-closed/inventory tests) prove a function <em>resolves and lowers</em>; this IT proves the
 * backend computes the right <em>value</em> — the one thing no off-cluster test can. It stays a
 * representative slice: exhaustive lowering coverage lives in the unit layer, so this covers each
 * aggregate, arithmetic, HAVING, CAST across AE types, string + date scalars, DISTINCT, join,
 * UNION, multi-key ORDER BY, LIMIT/OFFSET, and NULL handling with hand-authored golden rows.
 */
public class AnalyticsSqlFunctionMatrixIT extends AnalyticsRestTestCase {

    private static final String T = "sql_matrix";
    private static final String D = "sql_matrix_dim";
    private static boolean provisioned = false;

    private void ensureData() throws IOException {
        if (provisioned) return;
        // Type surface: integer, double, keyword/text, date, ip, scaled_float. 'nick' is sometimes null.
        createCompositeIndex(
            T,
            2,
            "{"
                + "\"dept\": {\"type\": \"keyword\"},"
                + "\"nick\": {\"type\": \"keyword\"},"
                + "\"salary\": {\"type\": \"long\"},"
                + "\"bonus\": {\"type\": \"double\"},"
                + "\"ratio\": {\"type\": \"scaled_float\", \"scaling_factor\": 100},"
                + "\"addr\": {\"type\": \"ip\"},"
                + "\"hired\": {\"type\": \"date\", \"format\": \"strict_date\"}"
                + "}"
        );
        StringBuilder bulk = new StringBuilder();
        // dept, nick, salary, bonus, ratio, addr, hired
        addDoc(bulk, "eng", "\"a\"", 100, 10.0, 0.5, "10.0.0.1", "2020-01-15");
        addDoc(bulk, "eng", "null", 120, 20.0, 0.25, "10.0.0.2", "2021-06-01");
        addDoc(bulk, "sales", "\"c\"", 90, 5.0, 0.75, "10.0.0.3", "2019-03-10");
        addDoc(bulk, "sales", "\"d\"", 80, 8.0, 0.1, "10.0.0.4", "2022-11-20");
        addDoc(bulk, "hr", "null", 70, 3.0, 0.9, "10.0.0.5", "2018-07-30");
        bulkAndRefresh(T, bulk.toString());

        createCompositeIndex(D, 1, "{\"dept\": {\"type\": \"keyword\"}, \"region\": {\"type\": \"keyword\"}}");
        StringBuilder dim = new StringBuilder();
        dim.append("{\"index\":{}}\n{\"dept\":\"eng\",\"region\":\"west\"}\n");
        dim.append("{\"index\":{}}\n{\"dept\":\"sales\",\"region\":\"east\"}\n");
        dim.append("{\"index\":{}}\n{\"dept\":\"hr\",\"region\":\"west\"}\n");
        bulkAndRefresh(D, dim.toString());
        provisioned = true;
    }

    private static void addDoc(StringBuilder bulk, String dept, String nickJson, int salary, double bonus, double ratio, String addr, String hired) {
        bulk.append("{\"index\":{}}\n");
        bulk.append("{\"dept\":\"").append(dept).append("\",")
            .append("\"nick\":").append(nickJson).append(",")
            .append("\"salary\":").append(salary).append(",")
            .append("\"bonus\":").append(bonus).append(",")
            .append("\"ratio\":").append(ratio).append(",")
            .append("\"addr\":\"").append(addr).append("\",")
            .append("\"hired\":\"").append(hired).append("\"}\n");
    }

    // ── Aggregates ────────────────────────────────────────────────────────────
    public void testAggregatesPerGroup() throws IOException {
        ensureData();
        SqlResult r = runAnalyticsSql(
            "SELECT dept, COUNT(*) c, SUM(salary) s, MIN(salary) mn, MAX(salary) mx, AVG(salary) av "
                + "FROM " + T + " GROUP BY dept ORDER BY dept"
        );
        // eng: 2 rows, sum 220, min 100, max 120, avg 110
        assertEquals(3, r.rows().size());
        assertEquals("eng", str(r, 0, "dept"));
        assertEquals(2L, num(r, 0, "c").longValue());
        assertEquals(220L, num(r, 0, "s").longValue());
        assertEquals(100L, num(r, 0, "mn").longValue());
        assertEquals(120L, num(r, 0, "mx").longValue());
        assertEquals(110.0, num(r, 0, "av").doubleValue(), 0.001);
    }

    // ── Arithmetic + HAVING ─────────────────────────────────────────────────────
    public void testArithmeticAndHaving() throws IOException {
        ensureData();
        SqlResult r = runAnalyticsSql(
            "SELECT dept, SUM(salary) + SUM(bonus) total FROM " + T + " GROUP BY dept HAVING SUM(salary) > 150 ORDER BY dept"
        );
        // SUM(salary): eng=220, sales=170, hr=70. HAVING > 150 keeps eng and sales, ordered by dept.
        // eng total = 220 + (10.0+20.0) = 250.0 ; sales total = 170 + (5.0+8.0) = 183.0
        assertEquals(2, r.rows().size());
        assertEquals("eng", str(r, 0, "dept"));
        assertEquals(250.0, num(r, 0, "total").doubleValue(), 0.001);
        assertEquals("sales", str(r, 1, "dept"));
        assertEquals(183.0, num(r, 1, "total").doubleValue(), 0.001);
    }

    // ── CAST across AE types ────────────────────────────────────────────────────
    public void testCast() throws IOException {
        ensureData();
        SqlResult r = runAnalyticsSql("SELECT CAST(salary AS DOUBLE) sd FROM " + T + " WHERE dept = 'hr'");
        assertEquals(1, r.rows().size());
        assertEquals(70.0, num(r, 0, "sd").doubleValue(), 0.001);
    }

    // ── String scalar ───────────────────────────────────────────────────────────
    public void testStringScalar() throws IOException {
        ensureData();
        SqlResult r = runAnalyticsSql("SELECT UPPER(dept) u FROM " + T + " WHERE dept = 'eng' GROUP BY dept");
        assertEquals("ENG", str(r, 0, "u"));
    }

    // ── DISTINCT ─────────────────────────────────────────────────────────────────
    public void testDistinct() throws IOException {
        ensureData();
        SqlResult r = runAnalyticsSql("SELECT DISTINCT dept FROM " + T + " ORDER BY dept");
        assertEquals(3, r.rows().size());
        assertEquals("eng", str(r, 0, "dept"));
        assertEquals("hr", str(r, 1, "dept"));
        assertEquals("sales", str(r, 2, "dept"));
    }

    // ── Join ───────────────────────────────────────────────────────────────────
    public void testJoin() throws IOException {
        ensureData();
        SqlResult r = runAnalyticsSql(
            "SELECT t.dept, d.region FROM " + T + " t JOIN " + D + " d ON t.dept = d.dept "
                + "WHERE t.dept = 'sales' GROUP BY t.dept, d.region"
        );
        assertEquals(1, r.rows().size());
        assertEquals("east", str(r, 0, "region"));
    }

    // ── Union ────────────────────────────────────────────────────────────────────
    public void testUnion() throws IOException {
        ensureData();
        SqlResult r = runAnalyticsSql(
            "SELECT dept FROM " + T + " WHERE salary >= 120 UNION SELECT dept FROM " + T + " WHERE salary <= 70 ORDER BY dept"
        );
        // salary>=120 → eng; salary<=70 → hr. Union → eng, hr.
        assertEquals(2, r.rows().size());
        assertEquals("eng", str(r, 0, "dept"));
        assertEquals("hr", str(r, 1, "dept"));
    }

    // ── Multi-key ORDER BY + LIMIT/OFFSET ──────────────────────────────────────────
    public void testOrderByLimitOffset() throws IOException {
        ensureData();
        SqlResult r = runAnalyticsSql("SELECT salary FROM " + T + " ORDER BY salary DESC LIMIT 2 OFFSET 1");
        // salaries desc: 120,100,90,80,70 → offset 1, limit 2 → 100, 90
        assertEquals(2, r.rows().size());
        assertEquals(100L, num(r, 0, "salary").longValue());
        assertEquals(90L, num(r, 1, "salary").longValue());
    }

    // ── NULL handling ──────────────────────────────────────────────────────────────
    public void testNullHandling() throws IOException {
        ensureData();
        SqlResult r = runAnalyticsSql("SELECT COUNT(*) c FROM " + T + " WHERE nick IS NULL");
        assertEquals(2L, num(r, 0, "c").longValue());
    }

    // ─── helpers ──────────────────────────────────────────────────────────────

    private record SqlResult(List<String> columns, List<List<Object>> rows) {}

    private static String str(SqlResult r, int row, String col) {
        return String.valueOf(r.rows().get(row).get(r.columns().indexOf(col)));
    }

    private static Number num(SqlResult r, int row, String col) {
        return (Number) r.rows().get(row).get(r.columns().indexOf(col));
    }

    @SuppressWarnings("unchecked")
    private SqlResult runAnalyticsSql(String sql) throws IOException {
        Request request = new Request("POST", "/_analytics/sql");
        request.setJsonEntity("{\"query\": \"" + escapeJson(sql) + "\"}");
        Response response = client().performRequest(request);
        Map<String, Object> body = assertOkAndParse(response, "SQL: " + sql);
        List<String> columns = (List<String>) body.get("columns");
        List<List<Object>> rows = (List<List<Object>>) body.get("rows");
        assertNotNull("response missing 'columns' for: " + sql, columns);
        assertNotNull("response missing 'rows' for: " + sql, rows);
        return new SqlResult(columns, rows);
    }

    private void createCompositeIndex(String name, int shards, String mappingProperties) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + name));
        } catch (Exception ignored) {}
        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": " + shards + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": { \"properties\": " + mappingProperties + " }"
            + "}";
        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "Create index " + name);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));
        Request health = new Request("GET", "/_cluster/health/" + name);
        health.addParameter("wait_for_status", "yellow");
        health.addParameter("timeout", "60s");
        client().performRequest(health);
    }

    private void bulkAndRefresh(String indexName, String bulkBody) throws IOException {
        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.setJsonEntity(bulkBody);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setOptions(bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        client().performRequest(bulkRequest);
        client().performRequest(new Request("POST", "/" + indexName + "/_flush?force=true"));
    }
}
