use obs_store::{Store, StoreConfig};
use obs_types::StoreError;

#[test]
fn migration_fresh_start_creates_schema_v1() {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig::new(dir.path().join("fresh.db"));
    let store = Store::open(&config).unwrap();
    let version: i64 = store
        .read_pool()
        .with_read(|conn| conn.query_row("SELECT version FROM schema_meta", [], |row| row.get(0)))
        .unwrap();
    assert_eq!(version, 1);

    // The full §3.1 surface exists.
    let tables: i64 = store
        .read_pool()
        .with_read(|conn| {
            conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN (
                   'schema_meta','events','runs','tree_nodes','score_points','checkpoints',
                   'findings','metric_series','metrics_raw','rollup_5s','rollup_1m','rollup_10m',
                   'rollup_state','coverage_cells','replays','alert_instances')",
                [],
                |row| row.get(0),
            )
        })
        .unwrap();
    assert_eq!(tables, 16);
}

#[test]
fn migration_restart_is_a_no_op() {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig::new(dir.path().join("restart.db"));
    drop(Store::open(&config).unwrap());
    let store = Store::open(&config).unwrap();
    let versions: i64 = store
        .read_pool()
        .with_read(|conn| conn.query_row("SELECT count(*) FROM schema_meta", [], |row| row.get(0)))
        .unwrap();
    assert_eq!(versions, 1, "restart must not re-run the migration");
}

#[test]
fn migration_refuses_newer_on_disk_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("future.db");
    let config = StoreConfig::new(&path);
    drop(Store::open(&config).unwrap());

    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("UPDATE schema_meta SET version = 99", [])
        .unwrap();
    drop(conn);

    let error = match Store::open(&config) {
        Ok(_) => panic!("open must refuse a newer on-disk schema version"),
        Err(error) => error,
    };
    match error {
        StoreError::SchemaTooNew { found, supported } => {
            assert_eq!(found, 99);
            assert_eq!(supported, 1);
        }
        other => panic!("expected SchemaTooNew, got {other:?}"),
    }
    let message = format!(
        "{}",
        StoreError::SchemaTooNew {
            found: 99,
            supported: 1
        }
    );
    assert!(message.contains("99") && message.contains('1'), "{message}");
}
