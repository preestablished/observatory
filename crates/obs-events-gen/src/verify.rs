//! Post-publish verification: regenerates the deterministic stream and
//! compares expected counts + an order-independent checksum against the
//! server's SQLite store (direct read via `--db`, acceptable for v1 per
//! the plan).

use std::path::Path;

use crate::sim::{Sim, SimConfig};

#[derive(Debug)]
pub struct VerifyReport {
    pub ok: bool,
    pub expected_valid: u64,
    pub stored: u64,
    pub expected_unknown: u64,
    pub stored_unknown: u64,
    pub checksum_expected: u64,
    pub checksum_stored: u64,
    pub mismatches: Vec<String>,
}

/// Stable 64-bit FNV-1a (no dependency; identical across builds).
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn event_digest(seq: u64, event_type: &str, payload: &[u8]) -> u64 {
    let mut buffer = Vec::with_capacity(payload.len() + event_type.len() + 8);
    buffer.extend_from_slice(&seq.to_le_bytes());
    buffer.extend_from_slice(event_type.as_bytes());
    buffer.push(0);
    buffer.extend_from_slice(payload);
    fnv1a(&buffer)
}

pub fn verify(
    db_path: &Path,
    config: SimConfig,
) -> Result<VerifyReport, Box<dyn std::error::Error>> {
    // Expected side: drain the deterministic sim, keeping valid envelopes
    // only (malformed payloads are rejected by the server by design).
    let mut expected_types: std::collections::BTreeMap<String, u64> = Default::default();
    let mut checksum_expected: u64 = 0;
    let mut expected_valid: u64 = 0;
    let run_id = config.run_id.clone();
    let mut sim = Sim::new(config);
    for envelope in sim.by_ref() {
        let malformed = serde_json::from_slice::<serde_json::Value>(&envelope.payload_json)
            .map(|value| !value.is_object())
            .unwrap_or(true);
        if malformed {
            continue;
        }
        expected_valid += 1;
        *expected_types
            .entry(envelope.event_type.clone())
            .or_insert(0) += 1;
        checksum_expected = checksum_expected.wrapping_add(event_digest(
            envelope.seq,
            &envelope.event_type,
            &envelope.payload_json,
        ));
    }
    let expected_unknown = sim.counts().unknown_type;

    // Stored side.
    let conn =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut stored: u64 = 0;
    let mut stored_unknown: u64 = 0;
    let mut stored_types: std::collections::BTreeMap<String, u64> = Default::default();
    let mut checksum_stored: u64 = 0;
    {
        let mut stmt =
            conn.prepare("SELECT seq, event_type, payload, unknown FROM events WHERE run_id = ?1")?;
        let mut rows = stmt.query([&run_id])?;
        while let Some(row) = rows.next()? {
            let seq: i64 = row.get(0)?;
            let event_type: String = row.get(1)?;
            let payload: String = row.get(2)?;
            let unknown: i64 = row.get(3)?;
            stored += 1;
            stored_unknown += unknown as u64;
            *stored_types.entry(event_type.clone()).or_insert(0) += 1;
            checksum_stored = checksum_stored.wrapping_add(event_digest(
                seq as u64,
                &event_type,
                payload.as_bytes(),
            ));
        }
    }

    let mut mismatches = Vec::new();
    if stored != expected_valid {
        mismatches.push(format!(
            "row count: stored {stored} != expected {expected_valid}"
        ));
    }
    if stored_unknown != expected_unknown {
        mismatches.push(format!(
            "unknown-flagged: stored {stored_unknown} != expected {expected_unknown}"
        ));
    }
    if checksum_stored != checksum_expected {
        mismatches.push(format!(
            "checksum: stored {checksum_stored:#x} != expected {checksum_expected:#x}"
        ));
    }
    for (event_type, expected_count) in &expected_types {
        let got = stored_types.get(event_type).copied().unwrap_or(0);
        if got != *expected_count {
            mismatches.push(format!(
                "count[{event_type}]: stored {got} != expected {expected_count}"
            ));
        }
    }

    Ok(VerifyReport {
        ok: mismatches.is_empty(),
        expected_valid,
        stored,
        expected_unknown,
        stored_unknown,
        checksum_expected,
        checksum_stored,
        mismatches,
    })
}
