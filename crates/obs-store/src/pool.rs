//! Read-only connection pool.
//!
//! Deviation from ARCHITECTURE §1 (which names `r2d2`): this is a
//! hand-rolled fixed pool of read-only connections — the plan sanctions
//! the substitution to avoid the rusqlite/r2d2 adapter version matrix;
//! recorded in the repo README. Connections are checked out under a std
//! mutex (waiting on a condvar when all are out) and returned after use;
//! WAL gives snapshot-isolated readers.

use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};

use rusqlite::{Connection, OpenFlags};

use obs_types::StoreError;

struct Shared {
    conns: Mutex<Vec<Connection>>,
    available: Condvar,
}

#[derive(Clone)]
pub struct ReadPool {
    shared: Arc<Shared>,
}

impl ReadPool {
    pub fn open(path: &Path, size: usize) -> Result<Self, StoreError> {
        let mut conns = Vec::with_capacity(size.max(1));
        for _ in 0..size.max(1) {
            let conn = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(|error| StoreError::Sqlite(error.to_string()))?;
            conn.execute_batch("PRAGMA query_only=ON; PRAGMA busy_timeout=5000;")
                .map_err(|error| StoreError::Sqlite(error.to_string()))?;
            conns.push(conn);
        }
        Ok(Self {
            shared: Arc::new(Shared {
                conns: Mutex::new(conns),
                available: Condvar::new(),
            }),
        })
    }

    /// Runs `f` with a pooled read-only connection on the current thread,
    /// blocking until one is available. Use from blocking contexts (or
    /// wrap in `spawn_blocking`).
    pub fn with_read<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, rusqlite::Error>,
    ) -> Result<T, StoreError> {
        let conn = {
            let mut conns = self
                .shared
                .conns
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            loop {
                if let Some(conn) = conns.pop() {
                    break conn;
                }
                conns = self
                    .shared
                    .available
                    .wait(conns)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        };
        let result = f(&conn).map_err(|error| StoreError::Sqlite(error.to_string()));
        {
            let mut conns = self
                .shared
                .conns
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            conns.push(conn);
        }
        self.shared.available.notify_one();
        result
    }
}
