/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Schema ↔ Arrow IPC byte conversion shared by the stage codec and FFM.
//!
//! The stream format (schema message + EOS) is the same one
//! `register_partition_stream` already ships across FFM, so a `StageReadExec`
//! schema serialized here is wire-compatible with the partition-stream schema
//! Java reads via `ArrowStreamReader`.

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{exec_datafusion_err, DataFusionError, Result};

/// Serialize a schema to Arrow IPC stream-format bytes (schema message + EOS).
pub fn schema_to_ipc(schema: &Schema) -> Result<Vec<u8>> {
    use arrow::ipc::writer::StreamWriter;
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| DataFusionError::Execution(format!("schema_to_ipc: writer: {e}")))?;
        writer
            .finish()
            .map_err(|e| DataFusionError::Execution(format!("schema_to_ipc: finish: {e}")))?;
    }
    Ok(buf)
}

/// Reconstruct a schema from Arrow IPC stream-format bytes.
pub fn schema_from_ipc(bytes: &[u8]) -> Result<SchemaRef> {
    use arrow::ipc::reader::StreamReader;
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| exec_datafusion_err!("schema_from_ipc: reader: {e}"))?;
    Ok(reader.schema())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn schema_round_trips() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("status", DataType::Utf8, true),
            Field::new("sum", DataType::Int64, true),
            Field::new("count", DataType::Int64, false),
        ]));
        let bytes = schema_to_ipc(&schema).unwrap();
        let back = schema_from_ipc(&bytes).unwrap();
        assert_eq!(schema.as_ref(), back.as_ref());
    }

    #[test]
    fn round_trips_binary_state_column() {
        // Engine-native-merge state column (D7) — single opaque Binary.
        let schema = Arc::new(Schema::new(vec![
            Field::new("status", DataType::Utf8, true),
            Field::new("hll_state", DataType::Binary, true),
        ]));
        let bytes = schema_to_ipc(&schema).unwrap();
        let back = schema_from_ipc(&bytes).unwrap();
        assert_eq!(schema.as_ref(), back.as_ref());
    }
}
