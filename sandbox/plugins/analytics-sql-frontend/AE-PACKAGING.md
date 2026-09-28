# AE packaging surface for the SQL front-end

This document tracks what `analytics-sql-frontend` consumes from Analytics Engine (AE) and
`analytics-api`, what this plugin newly exposed on the AE side, and the lower-stack adapters this
direct-Calcite path could eventually make redundant. It exists to answer R7 of the spec: "understand
what we need to package from AE to do this properly, and reduce the need for adapters lower down."

**Nothing has been removed.** Section 3 is a running, advisory list only.

## 1. Consumed AE / analytics-api surface

Everything the front-end needs is reached through the injected `analytics-api` SPI — the plugin has
**no** dependency on the `analytics-engine` plugin's internal classes, and **no** dependency on
`org.opensearch.sql.*` / `unified-query-*` (the coupling the PPL front-end carries and this plugin
deliberately avoids — R3).

| Surface | Where | How used |
|---|---|---|
| `EngineContextProvider` | `analytics-api` | Guice-injected into `SqlTransportAction`; yields the per-query context. |
| `EngineContextProvider.getContext()` → `QueryRequestContext` | `analytics-api` | Supplies the cluster-state-bound `SchemaPlus` used for parse/validate/convert and re-threaded into execution. |
| `EngineContextProvider.operatorTable()` | `analytics-api` | **New** (see §2). Supplies AE's function/operator table to the Calcite validator. |
| `QueryRequestContext(clusterState, schema, querySource)` | `analytics-api` | Rebuilt per query so planning and execution share one cluster-state view. |
| `QueryPlanExecutor<RelNode, Iterable<Object[]>>` | `analytics-api` | Guice-injected; executes the logical `RelNode`. `execute()` and `executeWithProfile()`. |
| `ProfiledResult` / `QueryProfile` | `analytics-api` (`exec.profile`) | The `_explain` path maps `QueryProfile` (stage timings) into the response — same semantics as the PPL front-end's `_explain`. |
| AE custom types (`IpType`, `UnsignedLongType`, `ScaledFloatType`, `DateOnlyType`, `TimeOnlyType`, `BinaryType`) | `analytics-api` (`schema/*`) | Consumed **indirectly**: `OpenSearchSchemaBuilder` builds each table's row type (including these UDTs) against the type factory the validator hands it, so they validate without a parallel type system. |
| `OpenSearchSchemaBuilder` (via the injected `SchemaPlus`) | `analytics-api` (`schema`) | The lazy, case-insensitive `getTable(name)` schema the front-end wraps and validates against. |
| Calcite `tools` API (`Frameworks`/`Planner`) + `calcite-core` | parent classloader (AE) | The parse → validate → convert pipeline. Provided at runtime by AE's classloader; `compileOnly` here. |

## 2. Newly exposed for this front-end (AE-side changes)

| Change | File | Why |
|---|---|---|
| `default SqlOperatorTable operatorTable()` added to the SPI | `analytics-api` `EngineContextProvider` | A front-end that parses SQL with Calcite directly must hand AE's operators to the validator. The default returns `SqlStdOperatorTable.instance()`, so existing extenders (the PPL front-end, which carries its own operator table) are unaffected. Additive. |
| `operatorTable()` overridden to return AE's real table | `analytics-engine` `AnalyticsPlugin.DefaultEngineContextProvider` | Returns `SqlOperatorTables.chain(aggregateOperatorTables(), SqlStdOperatorTable.instance())`. Reuses the existing `aggregateOperatorTables()` (made `static`, **not** removed or narrowed). Today `aggregateOperatorTables()` returns `SqlOperatorTables.of()` (an AE-side TODO to re-wire backend operators); when that is populated, this front-end picks up AE functions for free. |

**Type factory:** *no* SPI change was needed. `OpenSearchSchemaBuilder` builds each table's row type
against the `RelDataTypeFactory` the validator supplies at validation time
(`AbstractTable.getRowType(typeFactory)`), so a standard `JavaTypeFactoryImpl` in the front-end
resolves AE's custom UDTs correctly. A `typeFactory()` SPI method was considered and found
unnecessary.

## 3. Candidate adapter reductions (advisory — nothing removed yet)

Observations from building the direct-Calcite path, for a future cleanup pass:

- **`unified-query-*` parsing dependency (PPL front-end).** The PPL front-end pulls
  `unified-query-api/core/ppl` (the SQL plugin's artifacts) purely to turn text into a `RelNode`.
  Regular SQL no longer needs any of that — this plugin parses with Calcite directly. If/when PPL
  parsing is likewise migrated off `unified-query-*` (or PPL is served from the SQL plugin as the
  user intends), the `unified-query-*` dependency and the OpenSearch Snapshots repo it needs can be
  dropped from the front-end build entirely.
- **`aggregateOperatorTables()` — now wired (was an empty stub).** Previously returned an empty
  operator table with a TODO to "re-wire once operatorTable() is added back." It now delegates to
  `AnalyticsOperatorTables.build()`, which derives AE's published operator surface from the
  `ScalarFunction` / `AggregateFunction` / `WindowFunction` SPI enums. See §4 for the v1 boundary
  and the fail-closed contract this enables.
- **Type coercion adapters below the RelNode boundary.** The direct path hands AE a standard logical
  tree (`LogicalProject`/`Filter`/`Aggregate`/`Sort`/`Join`/`Union`) built against AE's own types, so
  any adapter that exists solely to reconcile the SQL plugin's type representations with AE's is not
  exercised on this path. Candidates to audit for removal once PPL also stops going through the SQL
  plugin — deferred until the end-to-end path is exercised on a full cluster (the ITs in
  `sandbox/qa/analytics-engine-rest` cover this once they can run in CI).

## 4. v1 function support boundary and the fail-closed contract

The front-end publishes AE's operator surface and rejects any standard SQL function AE cannot
execute — up front, before the plan reaches the executor — rather than letting it validate and
then fail opaquely deep in the native layer.

### Where the boundary is defined

`AnalyticsOperatorTables` (in `analytics-framework`) derives the boundary from the SPI enums —
the single source of truth for what AE recognizes:

- **Published table** (`build()`): the concrete Calcite operators the chained `SqlStdOperatorTable`
  does not already provide — AE scalars pinned to a reference operator whose name is a non-identifier
  symbol (e.g. `CONCAT` / `||`, `JSON_VALID` / `IS JSON VALUE`). Standard operators
  (`SUM`/`MIN`/`MAX`/`COUNT`/`AVG` and every `SqlKind`-resolved scalar) are **not** re-published: they
  already resolve through the chained std table, and wrapping a standard aggregate singleton ahead of
  the std table shadows its operand-type resolution.
- **`KNOWN_UNSUPPORTED_V1`**: every enum entry not resolvable by a validator — a conscious, listed
  omission rather than a silent gap. Cross-checked against `FunctionInventoryTests`, which fails if
  any enum entry is neither resolvable nor listed.

### v1 supported / unsupported (derived from the enums)

- **Aggregates — supported (7):** `SUM`, `SUM0`, `MIN`, `MAX`, `COUNT`, `AVG`, `APPROX_COUNT_DISTINCT`.
  **Unsupported (17):** `STDDEV_POP`/`STDDEV_SAMP`, `VAR_POP`/`VAR_SAMP`, `PERCENTILE_CONT`/
  `PERCENTILE_DISC`/`PERCENTILE_APPROX`, `COLLECT`, `LISTAGG`, `TAKE`, `FIRST`, `LAST`, `ARG_MIN`,
  `ARG_MAX`, `LIST`, `VALUES`, `PATTERN`. These are engine-native / state-expanding aggregates with no
  standard operator; a query using one is rejected with HTTP 400 naming it.
- **Scalars:** 37 resolve by a concrete `SqlKind` or a reference operator (comparisons, arithmetic,
  `CAST`/`CASE`/`COALESCE`, `CONCAT`, `POSITION`, `TRIM`, …). The remaining ~179 are name-only
  (`OTHER_FUNCTION`) UDFs — PPL/AE-specific functions a Calcite validator cannot reach by identifier
  — and are unsupported in v1's direct-SQL grammar.
- **Window functions — resolvable (11):** `SUM`/`AVG`/`COUNT`/`MIN`/`MAX`, `ARG_MIN`/`ARG_MAX`,
  `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTH_VALUE`. **Unsupported (2):** `DISTINCT_COUNT_APPROX`,
  `PATTERN`. The fail-closed walk covers window operators defensively even though v1's grammar does
  not commit to window functions.

### The fail-closed contract

`FunctionCapabilityWalker` walks the logical `RelNode` after convert and before execution, resolving
every scalar (`ScalarFunction.fromSqlOperatorWithFallback`), aggregate
(`AggregateFunction.fromSqlAggFunction`) and window operator (`WindowFunction.resolveFunction`)
through **AE's own resolution path** — the same seams AE's marking rules use — so it cannot drift
from what AE actually executes. An unresolvable function throws `UnsupportedFunctionException` (a
`400`, distinct from the unsupported-statement and parse errors), naming the offenders. There is no
parallel allowlist.

### Test coverage

- `FunctionInventoryTests` — drift guard: every enum entry is resolvable XOR listed unsupported.
- `FailClosedFunctionTests` — unit-level: an unsupported standard function is rejected before the
  executor is invoked; a supported query reaches it. Runs with **no Arrow on the test classpath**:
  `AggregateFunction`'s enum `<clinit>` no longer references `org.apache.arrow.ArrowType` (the two
  fixed intermediate-field types are encoded as an Arrow-free `AggregateFunction.FixedType`
  descriptor that the resolver maps straight to a Calcite type), so touching an aggregate constant
  no longer forces Arrow to load. Partial-aggregate resolution and lowering are therefore
  unit-testable off-cluster; Arrow is required only for actual execution (the ITs).
- `AnalyticsSqlFunctionMatrixIT` — live correctness (golden rows) across aggregates, arithmetic,
  `HAVING`, `CAST`, string scalars, `DISTINCT`, join, `UNION`, `ORDER BY`, `LIMIT`/`OFFSET`, NULLs.
- `AnalyticsSqlUnsupportedFunctionIT` — live fail-closed: an unsupported standard function returns
  `400` naming it; a supported function returns correct results.

### Known AE backend limitation surfaced by this work

`SUM` over a 32-bit `integer` column fails in the DataFusion reduce sink with *"Field … has a
different type (Int32) than the corresponding field in the table schema (Int64)"* — Calcite widens
`SUM(INTEGER)` to `BIGINT` but the reduce-stage Substrait schema keeps Int32. This is an
analytics-backend-datafusion defect (not a front-end / publish-table issue); the correctness IT sums
a `long` column to stay on the supported path. Worth a separate backend fix.
