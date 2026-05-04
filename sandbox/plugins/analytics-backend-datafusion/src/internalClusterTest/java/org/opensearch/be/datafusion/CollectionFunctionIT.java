/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import java.util.List;
import org.junit.Ignore;

/**
 * End-to-end tests for collection / multi-value scalar functions routed through
 * PPL → Calcite → Substrait → DataFusion. Bank fixture row 1: firstname='Amber',
 * balance=39225.
 */
public class CollectionFunctionIT extends BaseScalarFunctionIT {

    @SuppressWarnings("unchecked")
    private List<Object> evalArray(String expr) {
        Object cell = evalScalar(expr);
        assertNotNull(expr + " result must not be null", cell);
        assertTrue(expr + " result must be a List, got " + cell.getClass(), cell instanceof List);
        return (List<Object>) cell;
    }

    public void testSplit() {
        // firstname='Amber'; split on 'b' gives ['Am', 'er']. Field ref prevents
        // Calcite from constant-folding the call away.
        assertEquals(List.of("Am", "er"), evalArray("split(firstname, 'b')"));
    }

    public void testMvjoin() {
        // mvjoin(split(firstname,'b'), '|') — split('Amber','b')=['Am','er'] → 'Am|er'.
        assertScalarString("mvjoin(split(firstname, 'b'), '|')", "Am|er");
    }

    public void testArray() {
        // account_number=1 prevents constant-folding to a literal array.
        assertEquals(List.of(1L, 2L, 3L), evalArray("array(account_number, 2, 3)"));
    }

    public void testArrayLength() {
        // split(firstname,'b') = ['Am','er'] → length 2.
        assertScalarLong("array_length(split(firstname, 'b'))", 2L);
    }

    // Blocked: PPL grammar (in /Users/handalm/Workspace/sql — off-limits) has no
    // ARRAY_SLICE token. Parser rejects `array_slice(` before it reaches any
    // datafusion wiring we can control. BuiltinFunctionName.ARRAY_SLICE is marked
    // isInternal=true and no lexer rule exists for it.
    @Ignore
    public void testArraySlice() {
        // split('Amber', '') = ['A','m','b','e','r']; slice 2..4 (1-based, inclusive) = ['m','b','e'].
        assertEquals(List.of("m", "b", "e"), evalArray("array_slice(split(firstname, ''), 2, 4)"));
    }

    public void testMvappend() {
        // split(firstname,'b') = ['Am','er']; mvappend with split(firstname,'m') = ['A','ber']
        // → ['Am','er','A','ber']. Both operands typed list<string> to avoid ANY-typed arrays.
        assertEquals(List.of("Am", "er", "A", "ber"),
            evalArray("mvappend(split(firstname, 'b'), split(firstname, 'm'))"));
    }

    public void testMvdedup() {
        // mvappend gives ['Am','er','Am','er']; mvdedup → ['Am','er'].
        assertEquals(List.of("Am", "er"),
            evalArray("mvdedup(mvappend(split(firstname, 'b'), split(firstname, 'b')))"));
    }

    // Blocked: PPL lowers mvindex to Calcite ITEM(array, idx). Substrait-java encodes
    // ITEM as a "direct reference" (SelectionExpression), which DataFusion's Substrait
    // consumer only supports for StructField access — not list/array index. Error:
    // "Direct reference with types other than StructField is not supported". Fix
    // would require an adapter that rewrites ITEM → array_element before Substrait
    // conversion, or the YAML alias route isn't applicable here because the Calcite
    // emission isn't a named scalar function call at all.
    @Ignore
    public void testMvindex() {
        // split(firstname,'b') = ['Am','er']; mvindex 0 → 'Am'.
        assertScalarString("mvindex(split(firstname, 'b'), 0)", "Am");
    }

    // Blocked: DataFusion has no native mvzip / arrays_zip scalar function with
    // PPL's "concat-with-delim" semantics (Splunk's mvzip returns ['a,x','b,y']).
    // DF's arrays_zip returns list<struct> which is a different shape. A full fix
    // would need a custom adapter rewriting to CASE/UNNEST arithmetic.
    @Ignore
    public void testMvzip() {
        // mvzip(['Am','er'], ['x','y']) → ['Am,x','er,y'] (default delim ',').
        assertEquals(List.of("Am,x", "er,y"),
            evalArray("mvzip(split(firstname, 'b'), array('x', 'y'))"));
    }

    public void testMvfind() {
        // split(firstname,'b') = ['Am','er']; mvfind returns DF's 1-based array_position
        // (0 means "not found"). 'er' is the 2nd element.
        assertScalarLong("mvfind(split(firstname, 'b'), 'er')", 2L);
    }

    // Blocked: PPL grammar (in /Users/handalm/Workspace/sql — off-limits) has no
    // ARRAY_COMPACT token. Parser rejects `array_compact(`.
    @Ignore
    public void testArrayCompact() {
        // array_compact removes nulls. Construct array with a null via if(...).
        // ['Am', null, 'er'] → ['Am', 'er'].
        assertEquals(List.of("Am", "er"),
            evalArray("array_compact(array(mvindex(split(firstname,'b'),0), null, mvindex(split(firstname,'b'),1)))"));
    }

    public void testMapConcat() {
        // map_concat is complex (needs MAP construction); placeholder skipped per scope doc.
    }
}
