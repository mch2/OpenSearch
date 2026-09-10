# Explicit array type: implementation plan

Introduce a real array type, detected at ingestion from the first document a field appears in, and fixed
for the life of that index. Query contract: a scalar or aggregate function on an array is an error, and
`mvexpand` plus the array functions are how you get element-level behavior. **Nothing is rewritten behind
the user's back.** The UI and any other caller treat an array field as an array and emit array-aware
filters, functions and groupings.

The measured findings in `MULTI_VALUE_SEARCH_FINDINGS.md` hold, and the spikes in
`sandbox/plugins/analytics-backend-datafusion/rust/tests/mv_spikes.rs` apply unchanged.

# Constraints that remain

- **Rollover re-detection.** Each backing index derives its own mapping from its own first document, so
  `attributes.foo` can be `keyword` in `logs-000001` and array-of-keyword in `logs-000002`. A query
  across the data stream then spans both shapes, and the schema union has to resolve that rather than
  silently pick one (§A5, §E4).
- **Racy first-document detection.** With concurrent bulk indexing, which document creates the mapping
  is nondeterministic. Already true today for long-vs-double, so not new, but it means the producer
  guarantee is "be shape-consistent per field", not "send the array first".
- **The parquet LIST read bug.** Unrelated to semantics and blocks everything (§D).

# Decision needed before coding: the mapping syntax

Highest-leverage open question. Three shapes:

| option | mapping | cost |
|---|---|---|
| **A** — modifier on the existing type | `{"type":"keyword","array":true}` | smallest. Reuses #22883's parameter plumbing. Plugins that switch on `type` keep working |
| **B** — new type name per element type | `{"type":"keyword_array"}` | one new mapper, parser and type name per element type. Every consumer switching on `type` breaks |
| **C** — generic wrapper type | `{"type":"array","element_type":"keyword"}` | one new mapper. Every consumer reading `type` sees `array` and must then read `element_type` |

**Recommend A**, with the *type system* treating it as a genuine array even though the mapping spells it
as a modifier. `MappedFieldType` exposes array-ness, which flows into the Calcite row type and the
Arrow schema as a real `ARRAY`/`LIST`. That gets strong typing where it matters — planning, the schema
surfaced to clients, function resolution — at the least mapper churn.

Consequence to accept: `GET _mapping` shows `"array": true` rather than a type named `keyword[]`. If
the group wants the type name to be self-evident in the mapping output, that is option B or C and it is
a materially larger change.

# A. Core mapper: array-ness in the type system

| # | task | status |
|---|---|---|
| A1 | Array-ness on the field type as a two-state parameter, set at field creation and immutable thereafter | adapt #22883's parameter plumbing |
| A2 | Wire into the mappers that need it: keyword, long/integer/short/byte, double/float, boolean, date, ip | **done on `mv-search-experiments`** for long, integer, double, boolean, date, ip; keyword came with #22883 |
| A3 | Scalar value into an array field → singleton array | exists (#22883 LIST mode writes scalar input as a singleton) |
| A4 | Array value into a scalar field → `MapperParsingException` naming the field and the shape conflict | new |
| A5 | Cross-index conflict: array and scalar declarations of the same field within one index pattern fail at plan time with a named conflict, rather than `IndexResolution.validateSchemaCompatibility` judging them compatible and `resolveTable` picking one first-wins | new |
| A6 | Reject `array: true` on types with no parquet LIST writer (`text`) at index creation | exists in #22883's `ArrowSchemaBuilder` guard |
| A7 | Reject an array field as `index.sort.field` | exists (#22703) |
| A8 | `_source` / derived source preserves element order, duplicates, and `[]` distinct from absent | exists (#22703), needs a test per type |

# B. Core mapper: dynamic detection

The mechanism is dynamic mapping's first-non-null-value type resolution. Today an incoming `["a","b"]`
resolves from the *element* type and maps `keyword`, which Lucene tolerates because a posting list does
not care about cardinality. Parquet does.

| # | task | notes |
|---|---|---|
| B1 | Dynamic mapping distinguishes array-of-X from X during type detection and sets array-ness on the created field | the core of "autodetect using core code" |
| B2 | Detection applies array-ness automatically on top of whatever mapping a dynamic template specifies | **the OTel template needs no changes** — `attributes.*`, `resource.attributes.*` and `instrumentationScope.attributes.*` are wildcards and cannot enumerate which keys are arrays, so template declaration is not an option there |
| B3 | Optional `match_mapping_type` values for arrays, so a template can be explicit where it wants to be | nice-to-have; B2 covers OTel without it |
| B4 | Opt-out switch for templates that want to force scalar | decide whether needed |
| B5 | Concurrent creation: two documents racing to create the same field with different shapes resolves deterministically or rejects | pre-existing hazard for long-vs-double; decide the rule and test it |
| B6 | Rollover behavior: does the data stream carry the resolved mapping forward, or re-detect from the new index's first document | decide; drives whether A5 fires routinely or rarely |
| B7 | Rejection granularity: whole document, drop-field-and-index-the-rest, or route to the data-prepper DLQ | DLQ is the recoverable and countable option; needs an owner |

# C. Parquet storage

Largely in place already; the work is trimming it to a fixed-schema model.

| # | task | notes |
|---|---|---|
| C1 | Keep `LIST<element>` column storage, the generic `ParquetField.addToVector` / `supportsMultiValue` machinery, and LIST-aware encoding/compression/bloom config resolving to the `<field>.list.element` leaf | from #22883 / #22703 |
| C2 | Remove the mid-index schema-change path — writer rotation, `SchemaChangeRequiresWriterRotationException`, and the mapping-update trigger in `DocumentParser` / `ParametrizedFieldMapper`. A field's Arrow schema is fixed at creation, so every file in the index has one shape for it | simplification |
| C3 | Keep the raw-value companion column mirroring the parent's cardinality for derived source | from #22883 |

# D. Parquet read — blocks everything else

Today `source=idx \| fields tags` on a LIST column returns a 500. Parquet's physical layout is flat, so
an Arrow `List<Utf8>` field named `tags` lives at `tags.list.element` and nothing is named `tags`. The
scoped page-index optimization asks parquet-rs to map `tags` to a column number, gets "not found"
(parquet-rs refuses to guess for nested roots), and treats that as out-of-scope rather than
read-normally. The reader gets a placeholder byte range and the decompressor fails with `Src size is
incorrect` / `the offset to copy is not contained in the decompressed buffer`.

| # | task | notes |
|---|---|---|
| D1 | Bail out of the scoped page index for a segment whose projected columns did not all resolve | few lines; correct immediately, loses page-skipping on LIST columns only. **Do this first** |
| D2 | Resolve nested roots to every physical leaf beneath them, keeping the optimization | this is #22902's `cache/page_index/column_schema_resolver.rs` hunk |
| D3 | Row codec renders binary-typed array cells — `fields ip_m` currently returns `unsupported object class [B` | needed before `ip` arrays are claimed supported |
| D4 | Establish why `boolean` and `date` escape D1/D2 today, so the fix is understood rather than observed | open |

# E. Analytics engine: schema

| # | task | status |
|---|---|---|
| E1 | `OpenSearchSchemaBuilder` emits `ARRAY<element>` for array fields | **done on `mv-search-experiments`**, behind `analytics.multivalue.surface_arrays`; remove the toggle |
| E2 | `ArrowCalciteTypes.toArrowField` builds the element child — a childless Arrow `List` field cannot be allocated, and `LateMaterializationStageExecution` built exactly that | **done** |
| E3 | `ArrowValues` renders `ListVector` cells so `_source` reconstruction does not drop the column | **done** |
| E4 | Fail loudly on an index pattern mixing array and scalar declarations of one field (pairs with A5) | new |
| E5 | Response schema reports the array type. `AnalyticsExecutionEngine.buildSchema` derives from the pre-AE RelNode, so widen its existing `UNDEFINED` runtime-value type recovery to report `ARRAY` when a scalar-declared column's first cell is a collection | new; ~3 lines, in a method that already does this for `SCALAR_MAX`/`SCALAR_MIN` |

# F. Analytics engine: query semantics

No implicit rewriting. Every task here either rejects a shape or makes an explicitly written array
expression work.

| # | task | notes |
|---|---|---|
| F1 | Reject a scalar or aggregate function over an array with a message naming the array alternative. Largely free — the frontend's `PPLTypeChecker.validateOperands` already refuses ARRAY operands for every scalar function, for `SUM`/`AVG`/`PERCENTILE`/`VALUES`/`LIST`, and for `LIKE`, ranges and `=` | explicit work only where the type checker does not fire |
| F2 | Reject grouping over an array — `stats … by`, `top`, `rare`, `eventstats`, `streamstats`, window `PARTITION BY`. These type-check today and silently bucket the whole array, so each needs an explicit rejection | new; the only place the frontend does not reject for us |
| F3 | Reject `min` and `max` over an array. They currently fail inside the backend with an opaque error rather than a stated one | new |
| F4 | Reject an `mvexpand` positioned below a late-materialization anchor — row counts change and `___row_id` stitching assumes they do not | new |
| F5 | Verify what already works: `count(f)` counts documents holding a non-null list, and `distinct_count(f)` routes through `os_count_distinct`, which is list-aware on main | verify only |
| F6 | `FieldType.ARRAY` capability registrations for filter, project and aggregate, so an explicitly written array expression or a derived array column is not refused with "No backend can evaluate filter predicate" | new |
| F7 | Every rejection carries the array alternative in its message, and `explain` shows the array type on the column so a surprising result is diagnosable | new |

# G. mvexpand execution

The frontend half is already done: `mvexpand` is in `OpenSearchPPLParser.g4`, `MvExpand.java` and
`CalciteRelNodeVisitor.visitMvExpand`, which builds a **Correlate + Uncollect** — capture the outer row
in a `CorrelationId`, right side is `Values(one row)` → `uncollect` over the array field, then
`correlate(INNER, correlId, [arrayField])` with an optional `limit` for `limit=N`.

One trap: `visitMvExpand` returns the input unchanged when the field is not ARRAY-typed. So E1 is a
hard prerequisite — without it, `mvexpand tags` is a silent no-op.

| # | task | notes |
|---|---|---|
| G1 | Recognize the Correlate+Uncollect shape the frontend emits and mark it for the backend | mirrors `EngineCapability.MULTI_VALUE_EXPAND` on the other branch |
| G2 | Carry it across Substrait as an `ExtensionSingleRel` with an `Any{type_url:"os_unnest", value:{"column":…}}` detail. `DefaultSubstraitConsumer::consume_extension_single` already routes to `serializer_registry()`, which the AE reaches via `from_substrait_plan` | **spike-proven** — `spike_b2_extension_rel_carries_unnest_across_substrait` |
| G3 | Rust: `UserDefinedLogicalNodeCore` decoded from that detail, lowered to `LogicalPlan::Unnest` with `preserve_nulls=false` | spike-proven; move onto a production node |
| G4 | Unknown `type_url` errors rather than silently dropping the rel, so coordinator/data-node version skew cannot quietly change an answer | spike-proven |
| G5 | `limit=N` per-document cap | new |
| G6 | **Distributed stitching.** The other branch reverted here: Java POJO decoding loses `ExtensionSingleRel` output schema, and a proto-level attempt hit `No table named <source>`. `WholePlanStitcher` on `df-proto-migration` solves exactly this by stitching at POJO level through `ProtoPlanConverter`/`PlanProtoConverter` so extension anchors stay consistent | the hard one |

# H. Query frontend

Outside this repository. Step 9 in the order of work is the one that gates the UI story.

| # | task | notes |
|---|---|---|
| H1 | Expose `array_contains` and `cardinality` as callable functions | neither is in the PPL grammar or `PPLFuncImpTable` today, so **there is currently no way to write a membership filter on an array field**. `array_length`, `mvjoin`, `mvdedup`, `mvindex`, `mvzip` and `mvfind` already parse and already work against stored array columns |
| H2 | Report the array type in the response schema so callers can type the column. `AnalyticsExecutionEngine.buildSchema` derives it from the pre-execution RelNode; widening its existing `UNDEFINED` runtime-value recovery covers this | ~3 lines, in a method that already recovers types from values for `SCALAR_MAX`/`SCALAR_MIN` |
| H3 | Surface the rejection message so a caller sees the array alternative rather than a bare 400 | |
| H4 | `ARRAY[...]` literals and 1-based subscripts in the SQL grammar | lowest priority |

# I. UI and plugins

The UI treats an array field as an array. No scalar query is generated for an array column.

| # | task |
|---|---|
| I1 | Render array cells in the results table, preserving order and showing `[]` distinctly from absent |
| I2 | Field list marks array fields, so a user can see the shape before writing a query |
| I3 | Filter controls on an array field build a membership predicate rather than an equality one |
| I4 | Group-by on an array field emits `mvexpand` ahead of the aggregation |
| I5 | Array functions in the function picker, mapped to the DataFusion built-ins |
| I6 | Present the rejection message and its suggested array alternative inline rather than as a raw error |

# J. Test

| # | task |
|---|---|
| J1 | Repoint `MultiValueFunctionMatrixIT` at the array-type model: keep the scalar-versus-array comparison across every type, drop the arm that exercised mid-index schema change |
| J2 | Checked-in golden per tier — supported, documented, rejected — so a behavior change cannot land silently |
| J3 | Delegation agreement: run the filter matrix twice, delegation enabled and blocked, assert identical rows (F8) |
| J4 | Perf gate: expansion cost scales with total element count, and adding an unrelated scalar column to the projection does not multiply it (§E in `MULTI_VALUE_DESIGN.md`) |
| J5 | Ingestion round-trip per type: order, duplicates, `[]` versus absent, across refresh / flush / force-merge / node restart |
| J6 | Dynamic detection: array-first creates an array field, scalar-first then array rejects with a clear message, concurrent creation is deterministic |

# Order of work

| # | task | component |
|---|---|---|
| 1 | Resolve nested Arrow field names to their physical parquet leaves so LIST columns are readable | `analytics-backend-datafusion` (Rust) — `cache/page_index/column_schema_resolver.rs`, caller in `indexed_executor.rs` |
| 2 | Emit `ARRAY` in the Calcite row type and a complete `List` field in the Arrow schema | `analytics-api` — `OpenSearchSchemaBuilder`; `analytics-engine` — `ArrowCalciteTypes`, `ArrowValues`, `LateMaterializationStageExecution` |
| 3 | Declare array-ness on the field type and honour it in the parquet writer | `server` — `index/mapper` (`MappedFieldType`, `ParametrizedFieldMapper`, `FilterFieldType`, the keyword / number / boolean / date / ip mappers); `parquet-data-format` — `ParquetField` subclasses, `ArrowSchemaBuilder` |
| 4 | Retire the mid-index schema-change path so a field's Arrow schema is fixed at creation | `parquet-data-format` — `VSRManager`, `ParquetWriter`; `server` — `DocumentParser`, `ParametrizedFieldMapper` |
| 5 | Fail a document whose value shape contradicts the field's declared shape | `server` — `index/mapper` (`DocumentParser` and the leaf mappers) |
| 6 | Detect array-of-X during dynamic type resolution and stamp array-ness on the new field | `server` — `index/mapper` (dynamic-mapping path, `DynamicTemplate`, `ObjectMapper`) |
| 7 | Reject scalar functions, aggregates and grouping over arrays, each naming the array alternative | `analytics-engine` — `planner/rules` (`OpenSearchFilterRule`, `OpenSearchProjectRule`, `OpenSearchAggregateRule`) |
| 8 | Carry the frontend's Correlate + Uncollect across Substrait and execute it as an unnest | `analytics-engine` — planner recognition; `analytics-backend-datafusion` — extension-rel emission (Java) plus the logical node and serializer registry (Rust) |
| 9 | Expose array membership and cardinality functions so a filter on an array can be written at all — `array_contains` and `cardinality` are absent from the grammar today, while `array_length`, `mvjoin`, `mvdedup`, `mvindex`, `mvzip` and `mvfind` already work | opensearch-sql — PPL and SQL grammars, `PPLFuncImpTable` (external repo) |
| 10 | Register array capabilities so explicitly written array expressions plan and execute | `analytics-engine` — `planner/CapabilityRegistry`, `planner/rules`; `analytics-backend-datafusion` — `ScalarFunctionAdapter`, `opensearch_array_functions.yaml` |
| 11 | Preserve the unnest boundary's output schema across stage stitching so `mvexpand` works multi-shard | `analytics-backend-datafusion` — POJO-level plan stitching; `analytics-engine` — `planner/dag` |
| 12 | Report the array type in the response schema so callers can type the column | opensearch-sql — `AnalyticsExecutionEngine.buildSchema` (external repo) |
| 13 | Resolve an index pattern that spans array and scalar declarations of one field, or fail naming the conflict | `analytics-engine` — `planner/IndexResolution`; `analytics-api` — `OpenSearchSchemaBuilder.resolveTable` |
| 14 | Settle rollover carry-forward, concurrent field creation, and where rejected documents land | `server` — `index/mapper` and index metadata; OSIS / data-prepper for dead-letter routing |
| 15 | Render array cells, and build array-aware filters, functions and groupings rather than scalar ones | OpenSearch Dashboards |

Steps 1 and 2 gate the rest: nothing downstream is measurable while LIST columns cannot be read, and
`mvexpand` degrades to a silent no-op until arrays reach the frontend's row type. Step 9 gates the UI
story — until array membership is expressible, there is no way to write a filter on an array field.
Steps 9, 12 and 15 sit outside this repository. Test work tracks whichever step it covers.

# Open questions

1. **Mapping syntax** — option A, B or C above. Blocks the field-type work and every consumer of the
   mapping.
2. **Rollover** — carry the resolved mapping forward, or re-detect from the new index's first document?
   Decides whether cross-index shape conflicts are routine or rare.
3. **Rejection granularity** — document, field, or dead-letter queue, when a scalar-declared field
   receives an array.
4. **Is a 400 acceptable for grouping and filtering on an array until the UI is array-aware?** Until
   `array_contains` is callable there is no way to express a membership filter, and until the UI emits
   `mvexpand` there is no way to group per element. Both are 400s in the meantime, on queries that work
   today against a Lucene-backed index.
