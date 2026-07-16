#![forbid(unsafe_code)]
//! SQLite (WAL) storage layer: migration v1, the single-writer task, and a
//! read-only connection pool. Owns ALL SQL (ARCHITECTURE §1).
//!
//! Single-writer rule: exactly one task holds the write connection; every
//! other component sends it [`WriteBatch`] messages over a bounded mpsc.
//! Reads use [`ReadPool`] (WAL allows concurrent readers).

use std::path::{Path, PathBuf};

use rusqlite::Connection;

use obs_types::StoreError;

pub mod pool;
pub mod schema;
pub mod writer;

pub use pool::ReadPool;
pub use writer::{spawn_writer, WriterHandle, WRITER_CHANNEL_CAPACITY};

/// Storage-level options (subset of `observatoryd.toml` `[storage]`).
#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub path: PathBuf,
    /// Number of pooled read-only connections.
    pub read_pool_size: usize,
}

impl StoreConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            read_pool_size: 4,
        }
    }
}

/// An opened store: the write connection (to be handed to the writer task)
/// plus the read pool.
pub struct Store {
    write_conn: Connection,
    read_pool: ReadPool,
    path: PathBuf,
}

impl Store {
    /// Opens (creating if absent) the database, applies the write-side
    /// pragmas of ARCHITECTURE §3, and runs migrations. Refuses to open a
    /// database whose `schema_meta.version` is newer than this build.
    pub fn open(config: &StoreConfig) -> Result<Self, StoreError> {
        if let Some(parent) = config.path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|error| StoreError::Io(error.to_string()))?;
            }
        }
        let write_conn = Connection::open(&config.path).map_err(sqlite_error)?;
        apply_write_pragmas(&write_conn)?;
        migrate(&write_conn)?;
        let read_pool = ReadPool::open(&config.path, config.read_pool_size)?;
        Ok(Self {
            write_conn,
            read_pool,
            path: config.path.clone(),
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn read_pool(&self) -> ReadPool {
        self.read_pool.clone()
    }

    /// Splits the store into its writer-task input (the write connection)
    /// and the shared read pool.
    #[must_use]
    pub fn into_parts(self) -> (Connection, ReadPool) {
        (self.write_conn, self.read_pool)
    }
}

fn sqlite_error(error: rusqlite::Error) -> StoreError {
    StoreError::Sqlite(error.to_string())
}

/// Write-connection pragmas (ARCHITECTURE §3).
fn apply_write_pragmas(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA wal_autocheckpoint=4000;
         PRAGMA busy_timeout=5000;
         PRAGMA cache_size=-262144;
         PRAGMA mmap_size=268435456;",
    )
    .map_err(sqlite_error)
}

/// Applies migration v1 on a fresh database; is a no-op at the current
/// version; refuses to start on a newer on-disk version.
fn migrate(conn: &Connection) -> Result<(), StoreError> {
    let has_schema_meta: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_meta')",
            [],
            |row| row.get(0),
        )
        .map_err(sqlite_error)?;

    if !has_schema_meta {
        conn.execute_batch(&format!("BEGIN;\n{}\nCOMMIT;", schema::MIGRATION_V1.trim()))
            .map_err(sqlite_error)?;
        return Ok(());
    }

    let found: i64 = conn
        .query_row("SELECT version FROM schema_meta", [], |row| row.get(0))
        .map_err(sqlite_error)?;
    if found > schema::SCHEMA_VERSION {
        return Err(StoreError::SchemaTooNew {
            found,
            supported: schema::SCHEMA_VERSION,
        });
    }
    // found == SCHEMA_VERSION: no-op. Lower-but-valid versions appear only
    // once a migration v2 exists; v1 is the floor.
    Ok(())
}

/// Health probe: opens a NEW read-only connection per call — deliberately
/// not a pooled handle, because revoked filesystem permissions do not
/// affect already-open SQLite file descriptors (the /healthz chmod test
/// passes by construction only with a fresh open).
///
/// The plain `File::open` comes first because SQLite's unix VFS reuses
/// file descriptors already open on the same inode within this process
/// (the writer + read pool keep some alive), which would let a
/// permission-revoked database still "open" — only a real open(2) makes
/// the availability check honest.
pub fn probe(path: &Path) -> Result<(), StoreError> {
    std::fs::File::open(path).map_err(|error| StoreError::Io(error.to_string()))?;
    let conn = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(sqlite_error)?;
    let _version: i64 = conn
        .query_row("SELECT version FROM schema_meta", [], |row| row.get(0))
        .map_err(sqlite_error)?;
    Ok(())
}
