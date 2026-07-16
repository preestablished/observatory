//! Canonical byte serialization of every table, for the determinism gate
//! and idempotency tests. Every query carries an explicit ORDER BY over
//! the primary key — byte equality must never ride on SQLite's unordered
//! row return.

use rusqlite::types::ValueRef;
use rusqlite::Connection;

/// `(table, explicit primary-key ordering)` for all migration-v1 tables.
const TABLES: [(&str, &str); 16] = [
    ("schema_meta", "version"),
    ("events", "id"),
    ("runs", "run_id"),
    ("tree_nodes", "run_id, node_id"),
    ("score_points", "run_id, expansion_idx"),
    ("checkpoints", "run_id, checkpoint_id"),
    ("findings", "id"),
    ("metric_series", "series_id"),
    ("metrics_raw", "series_id, ts_ns"),
    ("rollup_5s", "series_id, bucket_ns"),
    ("rollup_1m", "series_id, bucket_ns"),
    ("rollup_10m", "series_id, bucket_ns"),
    ("rollup_state", "grain"),
    ("coverage_cells", "run_id, map_id, cx, cy"),
    ("replays", "artifact_id"),
    ("alert_instances", "id"),
];

/// Dumps every table into one canonical string.
pub fn dump_all(conn: &Connection) -> Result<String, rusqlite::Error> {
    let mut out = String::new();
    for (table, order) in TABLES {
        out.push_str("== ");
        out.push_str(table);
        out.push('\n');
        let mut stmt = conn.prepare(&format!("SELECT * FROM {table} ORDER BY {order}"))?;
        let column_count = stmt.column_count();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            for index in 0..column_count {
                if index > 0 {
                    out.push('|');
                }
                match row.get_ref(index)? {
                    ValueRef::Null => out.push_str("NULL"),
                    ValueRef::Integer(value) => out.push_str(&value.to_string()),
                    // {:?} prints f64 with full round-trip precision.
                    ValueRef::Real(value) => out.push_str(&format!("{value:?}")),
                    ValueRef::Text(text) => out.push_str(&String::from_utf8_lossy(text)),
                    ValueRef::Blob(blob) => {
                        out.push_str("x'");
                        for byte in blob {
                            out.push_str(&format!("{byte:02x}"));
                        }
                        out.push('\'');
                    }
                }
            }
            out.push('\n');
        }
    }
    Ok(out)
}
