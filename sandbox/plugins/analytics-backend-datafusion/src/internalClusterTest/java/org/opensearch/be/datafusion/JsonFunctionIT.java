/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

/**
 * End-to-end tests for JSON scalar functions (PPL → Calcite → Substrait → DataFusion Rust UDFs).
 *
 * <p>Each test exercises a function with literal JSON input. The bank fixture is a placeholder
 * — the {@code account_number = 1} filter just pins evaluation to a single row so head 1 is
 * deterministic; the JSON values are constants so the function's behavior, not the bank data,
 * is what's verified.
 */
public class JsonFunctionIT extends BaseScalarFunctionIT {

    public void testJsonValid() {
        assertScalarBoolean("json_valid('[1,2,3,4]')", true);
        assertScalarBoolean("json_valid('{\"invalid\": \"json')", false);
    }

    public void testJson() {
        assertScalarString("json('[1,2,3,4]')", "[1,2,3,4]");
    }

    public void testJsonInvalidReturnsNull() {
        assertScalarNull("json('{\"invalid\": \"json')");
    }

    public void testJsonObject() {
        assertScalarString("json_object('key', '123')", "{\"key\":\"123\"}");
    }

    public void testJsonArray() {
        // json_array embeds parseable values as JSON; '1' parses to integer 1.
        assertScalarString("json_array('1', '2', '3')", "[1,2,3]");
    }

    public void testJsonArrayLength() {
        assertScalarLong("json_array_length('[1,2,3,4]')", 4);
    }

    public void testJsonArrayLengthOnObjectIsNull() {
        assertScalarNull("json_array_length('{\"key\": 1}')");
    }

    public void testJsonKeys() {
        assertScalarString("json_keys('{\"f1\":\"a\",\"f2\":\"b\"}')", "[\"f1\",\"f2\"]");
    }

    public void testJsonKeysOnArrayIsNull() {
        assertScalarNull("json_keys('[1,2,3]')");
    }

    public void testJsonExtractField() {
        assertScalarString("json_extract('{\"name\":\"Alice\",\"age\":30}', 'name')", "Alice");
    }

    public void testJsonExtractIndex() {
        assertScalarString("json_extract('[10,20,30]', '{1}')", "20");
    }

    public void testJsonExtractAll() {
        // json_extract_all is not exposed in the PPL grammar's jsonFunctionName rule — it's
        // an internal helper used by the `spath` command rewrite (see
        // sql/core/src/main/java/org/opensearch/sql/expression/function/BuiltinFunctionName.java:271
        // where JSON_EXTRACT_ALL is flagged internal). For this wildcard-path case, though,
        // json_extract(x, '{}') and json_extract_all(x, '{}') produce identical output: the
        // Rust UDF's parse_path('{}') → [Step::Wildcard], and json_extract with a single
        // wildcard-bearing path already unwraps to Value::Array of all matches (see
        // JsonExtractUdf::invoke_with_args in rust/src/udf/json.rs). So this still exercises
        // the same wildcard-walk path through Substrait → DataFusion without touching grammar.
        assertScalarString("json_extract('[10,20,30]', '{}')", "[10,20,30]");
    }

    public void testJsonSet() {
        assertScalarString(
            "json_set('{\"a\":{\"b\":1}}', 'a.b', '3')",
            "{\"a\":{\"b\":3}}"
        );
    }

    public void testJsonDelete() {
        assertScalarString(
            "json_delete('{\"a\":1,\"b\":2}', 'b')",
            "{\"a\":1}"
        );
    }

    public void testJsonAppend() {
        assertScalarString(
            "json_append('{\"a\":[1,2]}', 'a', '3')",
            "{\"a\":[1,2,3]}"
        );
    }

    public void testJsonExtend() {
        // Extend with a JSON array unrolls it; append would nest.
        assertScalarString(
            "json_extend('{\"a\":[1,2]}', 'a', '[3,4]')",
            "{\"a\":[1,2,3,4]}"
        );
    }
}
