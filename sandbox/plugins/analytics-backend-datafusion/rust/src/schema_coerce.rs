/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Schema coercion at the Substrait/DataFusion scan boundary.
//!
//! Substrait's type system is narrower than Arrow's. The DataFusion Substrait
//! consumer rejects scans whose table-provider schema is not
//! `datatype_is_logically_equal` to the schema declared in the plan. The pairs
//! we hit on this path:
//!
//!   - `BinaryView`  ← parquet emits this for variable-length byte columns
//!     (including OpenSearch's 16-byte `ip` and `binary` mappings) when
//!     `schema_force_view_types` is on. Substrait has no view types — isthmus
//!     serializes `VARBINARY` as plain `binary`, which arrives as
//!     `DataType::Binary`.
//!
//!   - `UInt64`      ← parquet emits this for `unsigned_long` columns.
//!     Substrait integers are signed only — Calcite `BIGINT` serializes as
//!     `i64`, which arrives as `DataType::Int64`.
//!
//!   - `Float16`     ← parquet emits this for `half_float` columns. Calcite has
//!     no fp16 type; `OpenSearchSchemaBuilder` maps it to `REAL` which Substrait
//!     serializes as `fp32`, arriving as `DataType::Float32`.
//!
//! `Utf8View` doesn't need coercing: DataFusion 53's
//! `DFSchema::datatype_is_logically_equal` (in `datafusion-common/src/dfschema.rs`)
//! has hardcoded match arms `(Utf8, Utf8View) => true` and `(Utf8View, Utf8) => true`,
//! so a Substrait plan declaring `Utf8` binds cleanly against a table column
//! reporting `Utf8View`. The two cases above are different in nature:
//!
//!   - `(Binary, BinaryView)` is a missing equivalence in DataFusion. The two
//!     types are semantically identical, but `datatype_is_logically_equal` has
//!     no arm for them today (DF 53). If it becomes available in DataFusion,
//!     the `BinaryView → Binary` rewrite here can be removed.
//!
//!     TODO: every record batch goes through a `BinaryView → Binary` cast in the
//!     SchemaAdapter (offset+data buffer copy), and downstream operators see
//!     `Binary` rather than `BinaryView`. Drop this arm when we have a proper
//!     solution.
//!
//!   - `(Int64, UInt64)` is a Substrait + Calcite gap. Substrait's integer
//!     types are signed-only and Calcite has no unsigned `BIGINT`, so the
//!     unsigned semantics are lost before the plan reaches DataFusion. Values
//!     above `2^63 - 1` wrap into negatives — documented in
//!     `OpenSearchSchemaBuilder.mapFieldType`. Narrowing at the scan boundary
//!     is the only fix until Substrait grows unsigned types (see Substrait
//!     `proto/type.proto`).
//!
//!     TODO: values above `2^63 - 1` wrap into negatives. Drop this arm when we
//!     have a proper solution.
//!
//!   - `(Float32, Float16)` is a Calcite-side gap: Calcite has no fp16 type, so
//!     `half_float` columns are widened to `REAL` (fp32) at the Java planner.
//!     Every record batch goes through a `Float16 → Float32` cast in the
//!     SchemaAdapter, and downstream operators see `Float32` rather than
//!     `Float16` — so we lose the half-precision storage benefit at compute time.
//!
//!     TODO: every record batch goes through a `Float16 → Float32` cast and
//!     downstream operators see `Float32`. Drop this arm when we have a proper
//!     solution.
//!
//! We rewrite the inferred schema at the table-provider boundary: substitute the
//! Arrow-only types with their Substrait-compatible counterparts before handing
//! the schema to `ListingTableConfig`. The parquet reader's `SchemaAdapter` then
//! inserts the per-batch cast at read time (zero-copy bit reinterpret for
//! `UInt64 → Int64`; cheap buffer relabeling for `BinaryView → Binary`).
//!
//! Alternatives considered:
//!
//!   - Disable view types entirely with
//!     `execution.parquet.schema_force_view_types = false` (threaded into
//!     `ParquetFormat::with_options`). Makes the reader emit `Utf8/Binary` instead
//!     of `Utf8View/BinaryView`. Removes the BinaryView mismatch but also strips
//!     `Utf8View` from string columns, giving up the inline-prefix optimization
//!     on filter/group-by/hash paths.
//!
//!   - Skip `infer_schema` entirely and construct the Arrow schema directly from
//!     the OpenSearch mapping (the same source Calcite reads). Single source of
//!     truth, no post-process step. Costs the cross-language plumbing to ship
//!     the schema from Java to Rust and adds a second mapping table to keep in
//!     sync with `OpenSearchSchemaBuilder.mapFieldType`.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};

/// Convert Parquet string/binary fields to Arrow view types, including LIST children.
/// DataFusion's `transform_schema_to_view` only rewrites top-level fields, while the
/// coordinator declares `ARRAY<VARCHAR>` as `List<Utf8View>`.
pub fn transform_schema_to_view_recursive(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| Arc::new(rewrite_field_to_view(field)))
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

fn rewrite_field_to_view(field: &Field) -> Field {
    Field::new(
        field.name(),
        rewrite_data_type_to_view(field.data_type()),
        field.is_nullable(),
    )
    .with_metadata(field.metadata().clone())
}

fn rewrite_data_type_to_view(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => DataType::Utf8View,
        DataType::Binary | DataType::LargeBinary => DataType::BinaryView,
        DataType::List(child) => DataType::List(Arc::new(rewrite_field_to_view(child))),
        DataType::LargeList(child) => DataType::LargeList(Arc::new(rewrite_field_to_view(child))),
        // Deliberately NOT recursed into a struct. Substrait cannot express a view type, so a plan
        // always declares a nested string as `Utf8`; the reconciliation happens on the registered
        // side instead (`declare_nested_without_views`), and view-ifying here as well would just
        // move the mismatch rather than remove it.
        other => other.clone(),
    }
}

/// **The** conversion to use when turning a Substrait-declared schema into the physical Arrow
/// schema a data node actually produces. Prefer this over composing the pieces by hand.
///
/// Substrait has no view types — its type list is Boolean, I8..I64, FP32/64, String, Binary,
/// FixedChar, FixedBinary, Decimal, Date, Time, Timestamp, Interval*, UUID, Struct, List, Map and
/// UserDefined. Views are an Arrow *physical layout*, not a logical type, so any schema derived from
/// a declaration is view-less, while a data node running with `schema_force_view_types` emits
/// `Utf8View`/`BinaryView` — including inside LIST children.
///
/// Comparing the two directly is a recurring defect. `List<Utf8>` declared against `List<Utf8View>`
/// produced survives plan-time binding and then corrupts the Arrow C Data export, because the view
/// child's buffers are read as a `Utf8` child's offsets:
///
/// ```text
/// IllegalStateException: Offset buffer for type Utf8 is malformed: start: 4, end: 0
/// ```
///
/// reaching the user as `RefCnt has gone negative` from the failed import's cleanup. Scalars survive
/// only because DataFusion hardcodes `(Utf8, Utf8View) => true` binding compatibility — **and that
/// arm does not recurse**, so nested types get no such reprieve.
///
/// Two traps this exists to close:
///   1. DataFusion's own `transform_schema_to_view` rewrites top-level fields only, so a
///      `List<Utf8>` column passes through it unchanged. Use
///      [`transform_schema_to_view_recursive`], which this does.
///   2. The widening must precede [`coerce_inferred_schema`], whose narrowings would otherwise be
///      re-widened (or vice versa) depending on order.
///
/// Callers needing extra path-specific coercion (e.g. unsupported timestamp precisions) should apply
/// it to the input before calling, not reorder the steps here.
pub fn physical_schema_for_declaration(schema: &Schema, force_view_types: bool) -> SchemaRef {
    let widened = if force_view_types {
        transform_schema_to_view_recursive(schema)
    } else {
        schema.clone()
    };
    coerce_inferred_schema(Arc::new(widened))
}

/// Rewrite the schema to forms Substrait can bind against:
///   - `BinaryView` → `Binary`
///   - `UInt64`     → `Int64`
///   - `Float16`    → `Float32`
///
/// Recurses into `List`, `LargeList`, `FixedSizeList`, `Map`, `Struct`,
/// `Union`, and `Dictionary`. Returns the input unchanged when no rewrite is
/// needed so callers avoid an unnecessary `Arc` reallocation in the common
/// case.
pub fn coerce_inferred_schema(schema: SchemaRef) -> SchemaRef {
    if !schema_needs_coerce(&schema) {
        return schema;
    }
    let rewritten_fields: Vec<Field> = schema.fields().iter().map(|f| rewrite_field(f)).collect();
    Arc::new(Schema::new_with_metadata(
        rewritten_fields,
        schema.metadata().clone(),
    ))
}

fn schema_needs_coerce(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .any(|f| contains_incompatible(f.data_type()))
}

fn contains_incompatible(dt: &DataType) -> bool {
    match dt {
        DataType::BinaryView | DataType::UInt64 | DataType::Float16 => true,
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            contains_incompatible(f.data_type())
        }
        DataType::Map(f, _) => contains_incompatible(f.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| contains_incompatible(f.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, f)| contains_incompatible(f.data_type())),
        DataType::Dictionary(_, value_type) => contains_incompatible(value_type),
        _ => false,
    }
}

fn rewrite_field(field: &Field) -> Field {
    let new_type = rewrite_data_type(field.data_type());
    Field::new(field.name(), new_type, field.is_nullable()).with_metadata(field.metadata().clone())
}

fn rewrite_data_type(dt: &DataType) -> DataType {
    match dt {
        DataType::BinaryView => DataType::Binary,
        DataType::UInt64 => DataType::Int64,
        DataType::Float16 => DataType::Float32,
        DataType::List(f) => DataType::List(Arc::new(rewrite_field(f))),
        DataType::LargeList(f) => DataType::LargeList(Arc::new(rewrite_field(f))),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(Arc::new(rewrite_field(f)), *n),
        DataType::Map(f, sorted) => DataType::Map(Arc::new(rewrite_field(f)), *sorted),
        DataType::Struct(fields) => {
            let new_fields: Vec<Field> = fields.iter().map(|f| rewrite_field(f)).collect();
            DataType::Struct(Fields::from(new_fields))
        }
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(key.clone(), Box::new(rewrite_data_type(value)))
        }
        other => other.clone(),
    }
}

/// Rewrites view types to their non-view equivalents in NESTED positions only, for a schema about
/// to be compared against a Substrait plan's declaration.
///
/// Substrait has no view types, so a plan always declares a nested string as `Utf8` while parquet
/// reports `Utf8View`. DataFusion hardcodes `(Utf8, Utf8View)` as binding-compatible, but that arm
/// does not recurse: inside a struct the child list *is* the type, so one `Utf8View` child against a
/// declared `Utf8` fails the whole column —
///
/// ```text
/// table:     "path": Utf8View, "name": Utf8View
/// substrait: "path": Utf8,     "name": Utf8
/// ```
///
/// Top-level fields are left alone: they already bind through that hardcoded arm, and rewriting them
/// is unnecessary risk.
///
/// Deliberately NOT part of [`coerce_inferred_schema`]. That helper also types aggregate `List` state
/// — `list()`, `values()`, distinct-count bitmaps — and rewriting a view inside those corrupts them,
/// which shows up as two dozen scalar multi-value failures. Only a schema being derived for
/// declaration comparison may be rewritten this way.
pub fn declare_nested_without_views(schema: &Schema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            // Composite only — a top-level scalar keeps whatever parquet reported.
            DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) => {
                Field::new(f.name(), strip_views(f.data_type()), f.is_nullable())
                    .with_metadata(f.metadata().clone())
            }
            _ => f.as_ref().clone(),
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn strip_views(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::Struct(children) => DataType::Struct(Fields::from(
            children
                .iter()
                .map(|c| strip_views_field(c))
                .collect::<Vec<Field>>(),
        )),
        DataType::List(child) => DataType::List(Arc::new(strip_views_field(child))),
        DataType::LargeList(child) => DataType::LargeList(Arc::new(strip_views_field(child))),
        DataType::Map(child, sorted) => DataType::Map(Arc::new(strip_views_field(child)), *sorted),
        other => other.clone(),
    }
}

fn strip_views_field(field: &Field) -> Field {
    Field::new(
        field.name(),
        strip_views(field.data_type()),
        field.is_nullable(),
    )
    .with_metadata(field.metadata().clone())
}

/// Widens `registered` so it covers every field `expected` declares, as nullable.
/// `Some(augmented)` if anything was added, `None` if `registered` already covers `expected`.
///
/// The Substrait consumer binds `base_schema` to the provider BY NAME, so the registered schema
/// only needs to *contain* every expected column — order is irrelevant and present columns keep
/// their inferred (coerced) types. Added fields are forced nullable; DataFusion's parquet
/// `SchemaAdapter` null-fills them at read time.
///
/// # Nested columns
///
/// The two schemas come from different places: `expected` from the cluster-state mapping, which is
/// the union of every field ever seen, and `registered` from the parquet files on this shard, which
/// only contain what their own documents carried. A field the mapping has and the files do not used
/// to be a missing top-level column, which appending covered.
///
/// An OpenSearch `object` is one struct column, and for a struct the child list *is* the type — so a
/// child this shard never saw is not a missing column but a type mismatch on a column present in
/// both, which the plan rejects with "Field 'attributes' … different type". Appending alone would
/// leave it, so a field present in both is rebuilt by unioning its children, recursing all the way
/// down. An array of objects needs the same one level deeper: `LIST<STRUCT<..>>` unions the element's
/// children, which is what an OpenTelemetry `events` array hits as soon as one segment's events
/// carry an attribute another's do not.
pub fn append_missing_nullable(registered: &Schema, expected: &Schema) -> Option<SchemaRef> {
    let mut fields: Vec<Field> = Vec::with_capacity(registered.fields().len());
    let mut changed = false;
    for rf in registered.fields() {
        match expected.field_with_name(rf.name()) {
            Ok(ef) => match widen_to_cover(rf, ef) {
                Some(widened) => {
                    fields.push(widened);
                    changed = true;
                }
                None => fields.push(rf.as_ref().clone()),
            },
            Err(_) => fields.push(rf.as_ref().clone()),
        }
    }
    for ef in expected.fields() {
        if registered.field_with_name(ef.name()).is_err() {
            fields.push(
                Field::new(ef.name(), ef.data_type().clone(), true)
                    .with_metadata(ef.metadata().clone()),
            );
            changed = true;
        }
    }
    if changed == false {
        return None;
    }
    Some(Arc::new(Schema::new_with_metadata(
        fields,
        registered.metadata().clone(),
    )))
}

/// Rebuilds `registered` to also cover the children `expected` declares, or `None` when it already
/// does. Only composite types can differ this way — a scalar is covered or it is a different type,
/// which is not this function's problem.
fn widen_to_cover(registered: &Field, expected: &Field) -> Option<Field> {
    match (registered.data_type(), expected.data_type()) {
        (DataType::Struct(rc), DataType::Struct(ec)) => {
            let children = union_children(rc, ec)?;
            Some(
                Field::new(
                    registered.name(),
                    DataType::Struct(children),
                    registered.is_nullable(),
                )
                .with_metadata(registered.metadata().clone()),
            )
        }
        (DataType::List(re), DataType::List(ee)) => {
            let element = widen_to_cover(re, ee)?;
            Some(
                Field::new(
                    registered.name(),
                    DataType::List(Arc::new(element)),
                    registered.is_nullable(),
                )
                .with_metadata(registered.metadata().clone()),
            )
        }
        (DataType::LargeList(re), DataType::LargeList(ee)) => {
            let element = widen_to_cover(re, ee)?;
            Some(
                Field::new(
                    registered.name(),
                    DataType::LargeList(Arc::new(element)),
                    registered.is_nullable(),
                )
                .with_metadata(registered.metadata().clone()),
            )
        }
        _ => None,
    }
}

/// Rebuilds `registered`'s child list to match `expected` — same members, same ORDER.
///
/// `DataType::Struct` equality compares the ordered field list, so identical children in a different
/// order are different types. The two orders differ by construction and not by luck: the plan's comes
/// from the cluster-state mapping, which OpenSearch stores sorted, while the file's is the order the
/// writer first saw each attribute. For OpenTelemetry attributes that is immediate — two segments
/// almost never see the same keys first.
///
/// So iterate `expected` and take the registered child where it exists (keeping its inferred, coerced
/// type) or a nullable placeholder where it does not. Order then matches by construction rather than
/// by coincidence. A child only the file has is appended after; it cannot be dropped without losing
/// data, and in practice does not occur since the mapping is the union of every field ever seen.
///
/// `None` when the result is identical to `registered`, so an unchanged column keeps its original
/// `Field` and identity.
fn union_children(registered: &Fields, expected: &Fields) -> Option<Fields> {
    let mut children: Vec<Field> = Vec::with_capacity(expected.len());
    for ec in expected {
        match registered.find(ec.name()) {
            Some((_, rc)) => children.push(widen_to_cover(rc, ec).unwrap_or_else(|| rc.as_ref().clone())),
            None => children.push(
                Field::new(ec.name(), ec.data_type().clone(), true)
                    .with_metadata(ec.metadata().clone()),
            ),
        }
    }
    for rc in registered {
        if expected.find(rc.name()).is_none() {
            children.push(rc.as_ref().clone());
        }
    }
    let rebuilt = Fields::from(children);
    if &rebuilt == registered {
        return None;
    }
    Some(rebuilt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_missing_adds_absent_columns_as_nullable() {
        let registered = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]);
        // `alias` is declared non-nullable in the plan; it must still be appended as nullable
        // since the shard has no data for it.
        let expected = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("alias", DataType::Utf8, false),
        ]);

        let merged =
            append_missing_nullable(&registered, &expected).expect("alias missing → augmented");
        assert_eq!(merged.fields().len(), 3);
        let alias = merged.field_with_name("alias").unwrap();
        assert_eq!(alias.data_type(), &DataType::Utf8);
        assert!(alias.is_nullable(), "appended column must be nullable");
        assert!(merged.field_with_name("name").is_ok());
        assert!(merged.field_with_name("age").is_ok());
    }

    #[test]
    fn append_missing_unions_struct_children() {
        // The mapping is the union of every field ever seen; a shard's parquet files only carry what
        // their own documents had. For a struct the child list IS the type, so a child the file never
        // saw makes the whole column a type mismatch at plan binding — not a missing column that the
        // top-level pass could append.
        let registered = Schema::new(vec![struct_field("attributes", vec![utf8("http")])]);
        let expected = Schema::new(vec![struct_field(
            "attributes",
            vec![utf8("http"), utf8("cart")],
        )]);

        let merged = append_missing_nullable(&registered, &expected)
            .expect("a struct child the shard never saw must widen the column");
        let DataType::Struct(children) = merged.field_with_name("attributes").unwrap().data_type()
        else {
            panic!("attributes must stay a struct");
        };
        assert_eq!(children.len(), 2, "children are unioned, not replaced");
        let cart = children.find("cart").expect("absent child is added").1;
        assert!(cart.is_nullable(), "an added child must be nullable");
        assert!(children.find("http").is_some(), "existing child is kept");
    }

    #[test]
    fn append_missing_unions_struct_children_inside_a_list() {
        // An array of objects has the same problem one level deeper: `events` is a LIST<STRUCT<..>>,
        // and one segment's events carrying an attribute another's did not is a child mismatch.
        let element = |children: Vec<Field>| {
            Field::new("element", DataType::Struct(Fields::from(children)), true)
        };
        let registered = Schema::new(vec![Field::new(
            "events",
            DataType::List(Arc::new(element(vec![utf8("name")]))),
            true,
        )]);
        let expected = Schema::new(vec![Field::new(
            "events",
            DataType::List(Arc::new(element(vec![utf8("name"), utf8("reason")]))),
            true,
        )]);

        let merged = append_missing_nullable(&registered, &expected)
            .expect("an element child the shard never saw must widen the list");
        let DataType::List(el) = merged.field_with_name("events").unwrap().data_type() else {
            panic!("events must stay a list");
        };
        let DataType::Struct(children) = el.data_type() else {
            panic!("elements must stay structs");
        };
        assert_eq!(children.len(), 2);
        assert!(children.find("reason").expect("absent child is added").1.is_nullable());
    }

    #[test]
    fn append_missing_recurses_into_a_sub_object() {
        // `city.geo.zone` — the absent child is a grandchild, so the union has to recurse.
        let registered = Schema::new(vec![struct_field(
            "city",
            vec![struct_field("geo", vec![utf8("country")])],
        )]);
        let expected = Schema::new(vec![struct_field(
            "city",
            vec![struct_field("geo", vec![utf8("country"), utf8("zone")])],
        )]);

        let merged = append_missing_nullable(&registered, &expected).expect("grandchild widens");
        let DataType::Struct(city) = merged.field_with_name("city").unwrap().data_type() else {
            panic!()
        };
        let DataType::Struct(geo) = city.find("geo").unwrap().1.data_type() else {
            panic!()
        };
        assert_eq!(geo.len(), 2);
        assert!(geo.find("zone").unwrap().1.is_nullable());
    }

    #[test]
    fn struct_children_are_reordered_to_the_expected_order() {
        // Identical membership, different order. The plan's order is the mapping's (sorted); the
        // file's is the order the writer first saw each key. `DataType::Struct` equality compares the
        // ordered list, so this alone fails the whole column.
        let registered = Schema::new(vec![struct_field(
            "attributes",
            vec![utf8("rpc"), utf8("app"), utf8("net")],
        )]);
        let expected = Schema::new(vec![struct_field(
            "attributes",
            vec![utf8("app"), utf8("net"), utf8("rpc")],
        )]);

        let merged = append_missing_nullable(&registered, &expected)
            .expect("same children in a different order still needs rebuilding");
        let DataType::Struct(children) = merged.field_with_name("attributes").unwrap().data_type()
        else {
            panic!("attributes must stay a struct");
        };
        assert_eq!(
            children.iter().map(|c| c.name().as_str()).collect::<Vec<_>>(),
            vec!["app", "net", "rpc"],
            "children must come out in the expected schema's order"
        );
    }

    #[test]
    fn struct_children_are_reordered_inside_a_list_element() {
        // Same one level deeper, which an array of objects hits as soon as two segments see event
        // attributes in a different order.
        let element = |children: Vec<Field>| {
            Field::new("element", DataType::Struct(Fields::from(children)), true)
        };
        let list_of =
            |children: Vec<Field>| Field::new("events", DataType::List(Arc::new(element(children))), true);
        let registered = Schema::new(vec![list_of(vec![utf8("reason"), utf8("name")])]);
        let expected = Schema::new(vec![list_of(vec![utf8("name"), utf8("reason")])]);

        let merged = append_missing_nullable(&registered, &expected).expect("element order rebuilt");
        let DataType::List(el) = merged.field_with_name("events").unwrap().data_type() else {
            panic!()
        };
        let DataType::Struct(children) = el.data_type() else {
            panic!()
        };
        assert_eq!(
            children.iter().map(|c| c.name().as_str()).collect::<Vec<_>>(),
            vec!["name", "reason"]
        );
    }

    #[test]
    fn reordering_keeps_the_registered_childs_inferred_type() {
        // The point of taking the registered child rather than the expected one: its type is the
        // coerced/physical one, which the plan's declaration may not match exactly.
        let registered = Schema::new(vec![struct_field(
            "attributes",
            vec![
                Field::new("count", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
            ],
        )]);
        let expected = Schema::new(vec![struct_field(
            "attributes",
            vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("count", DataType::Int64, false),
            ],
        )]);

        let merged = append_missing_nullable(&registered, &expected).expect("reordered");
        let DataType::Struct(children) = merged.field_with_name("attributes").unwrap().data_type()
        else {
            panic!()
        };
        assert_eq!(
            children.iter().map(|c| c.name().as_str()).collect::<Vec<_>>(),
            vec!["name", "count"]
        );
        assert!(
            children.find("count").unwrap().1.is_nullable(),
            "the registered child is kept as-is, not replaced by the declaration"
        );
    }

    #[test]
    fn append_missing_returns_none_when_struct_children_already_match() {
        let registered = Schema::new(vec![struct_field(
            "attributes",
            vec![utf8("http"), utf8("cart")],
        )]);
        // Registered may carry a child the plan does not reference — still nothing to add.
        let expected = Schema::new(vec![struct_field("attributes", vec![utf8("http")])]);
        assert!(append_missing_nullable(&registered, &expected).is_none());
    }

    #[test]
    fn nested_views_are_declared_without_views() {
        // The exact shape of the failure: the files report Utf8View children while the plan, which
        // cannot express a view, declares Utf8. DataFusion's (Utf8, Utf8View) binding arm does not
        // recurse, so one view child fails the whole column.
        let registered = Schema::new(vec![
            Field::new("traceId", DataType::Utf8View, true),
            struct_field(
                "attributes",
                vec![
                    Field::new("path", DataType::Utf8View, true),
                    Field::new("name", DataType::Utf8View, true),
                ],
            ),
        ]);

        let declared = declare_nested_without_views(&registered);

        let DataType::Struct(children) = declared.field_with_name("attributes").unwrap().data_type()
        else {
            panic!("attributes must stay a struct");
        };
        assert_eq!(children.find("path").unwrap().1.data_type(), &DataType::Utf8);
        assert_eq!(children.find("name").unwrap().1.data_type(), &DataType::Utf8);
        assert_eq!(
            declared.field_with_name("traceId").unwrap().data_type(),
            &DataType::Utf8View,
            "a top-level scalar is left alone — it already binds through DataFusion's own arm"
        );
    }

    #[test]
    fn nested_views_are_stripped_inside_a_list_element_and_at_depth() {
        let element = Field::new(
            "element",
            DataType::Struct(Fields::from(vec![
                Field::new("name", DataType::Utf8View, true),
                Field::new(
                    "attributes",
                    DataType::Struct(Fields::from(vec![Field::new(
                        "reason",
                        DataType::Utf8View,
                        true,
                    )])),
                    true,
                ),
            ])),
            true,
        );
        let registered = Schema::new(vec![Field::new(
            "events",
            DataType::List(Arc::new(element)),
            true,
        )]);

        let declared = declare_nested_without_views(&registered);
        let DataType::List(el) = declared.field_with_name("events").unwrap().data_type() else {
            panic!()
        };
        let DataType::Struct(children) = el.data_type() else {
            panic!()
        };
        assert_eq!(children.find("name").unwrap().1.data_type(), &DataType::Utf8);
        let DataType::Struct(attrs) = children.find("attributes").unwrap().1.data_type() else {
            panic!()
        };
        assert_eq!(
            attrs.find("reason").unwrap().1.data_type(),
            &DataType::Utf8,
            "stripping reaches a grandchild"
        );
    }

    #[test]
    fn aggregate_list_state_is_untouched_by_the_shared_coercion() {
        // coerce_inferred_schema also types aggregate List state — list(), values(), distinct-count
        // bitmaps. Rewriting a view inside those corrupts them, which is why the view stripping lives
        // in its own declaration-only helper and NOT here.
        let state = Schema::new(vec![Field::new(
            "list_state",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8View, true))),
            true,
        )]);
        let coerced = coerce_inferred_schema(Arc::new(state));
        let DataType::List(item) = coerced.field_with_name("list_state").unwrap().data_type() else {
            panic!()
        };
        assert_eq!(
            item.data_type(),
            &DataType::Utf8View,
            "the shared helper must not rewrite a view in aggregate state"
        );
    }


    fn utf8(name: &str) -> Field {
        Field::new(name, DataType::Utf8, true)
    }

    fn struct_field(name: &str, children: Vec<Field>) -> Field {
        Field::new(name, DataType::Struct(Fields::from(children)), true)
    }

    #[test]
    fn append_missing_returns_none_when_registered_covers_expected() {
        let registered = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]);
        // Registered may carry extra columns the plan doesn't reference — still nothing to add.
        let expected = Schema::new(vec![Field::new("name", DataType::Utf8, true)]);
        assert!(append_missing_nullable(&registered, &expected).is_none());
    }

    #[test]
    fn top_level_binary_view_gets_rewritten() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::BinaryView, true),
        ]));
        let out = coerce_inferred_schema(schema);
        assert_eq!(out.field(0).data_type(), &DataType::Int64);
        assert_eq!(out.field(1).data_type(), &DataType::Binary);
    }

    #[test]
    fn top_level_uint64_gets_rewritten() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let out = coerce_inferred_schema(schema);
        assert_eq!(out.field(0).data_type(), &DataType::Int64);
        assert_eq!(out.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn schema_without_incompatible_types_is_returned_unchanged() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8View, true),
        ]));
        let before = Arc::as_ptr(&schema);
        let out = coerce_inferred_schema(schema);
        assert_eq!(
            Arc::as_ptr(&out),
            before,
            "unchanged schema must not reallocate"
        );
    }

    #[test]
    fn nested_list_of_binary_view_gets_rewritten() {
        let inner = Field::new("item", DataType::BinaryView, true);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "xs",
            DataType::List(Arc::new(inner)),
            true,
        )]));
        let out = coerce_inferred_schema(schema);
        match out.field(0).data_type() {
            DataType::List(f) => assert_eq!(f.data_type(), &DataType::Binary),
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn nested_list_of_uint64_gets_rewritten() {
        let inner = Field::new("item", DataType::UInt64, true);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "xs",
            DataType::List(Arc::new(inner)),
            true,
        )]));
        let out = coerce_inferred_schema(schema);
        match out.field(0).data_type() {
            DataType::List(f) => assert_eq!(f.data_type(), &DataType::Int64),
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn field_metadata_and_nullability_preserved() {
        let mut md = std::collections::HashMap::new();
        md.insert("key".to_string(), "value".to_string());
        let f = Field::new("b", DataType::BinaryView, false).with_metadata(md.clone());
        let schema = Arc::new(Schema::new(vec![f]));
        let out = coerce_inferred_schema(schema);
        assert!(!out.field(0).is_nullable());
        assert_eq!(out.field(0).metadata(), &md);
    }

    #[test]
    fn utf8_view_is_left_alone() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8View, true),
            Field::new("b", DataType::BinaryView, true),
        ]));
        let out = coerce_inferred_schema(schema);
        assert_eq!(out.field(0).data_type(), &DataType::Utf8View);
        assert_eq!(out.field(1).data_type(), &DataType::Binary);
    }

    #[test]
    fn other_unsigned_ints_are_left_alone() {
        // Only UInt64 ↔ Int64 needs coercion for OpenSearch's unsigned_long
        // mapping today. Smaller unsigned widths aren't produced by any current
        // OpenSearch field type, and Calcite has no SMALLINT-equivalent
        // unsigned type to mismatch against, so we leave them untouched.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt8, true),
            Field::new("b", DataType::UInt16, true),
            Field::new("c", DataType::UInt32, true),
        ]));
        let before = Arc::as_ptr(&schema);
        let out = coerce_inferred_schema(schema);
        assert_eq!(Arc::as_ptr(&out), before);
    }
}
