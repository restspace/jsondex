use std::env;
use std::fs;
use std::io;
use std::os::raw::c_int;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use rusqlite::ffi;
use rusqlite::types::Null;
use rusqlite::vtab::{
    read_only_module, Context, CreateVTab, IndexInfo, VTab, VTabConnection, VTabCursor, VTabKind,
    Values,
};
use rusqlite::{Connection, Result};
use serde_json::Value;

#[derive(Debug, Clone)]
struct SchemaConfig {
    path: PathBuf,
    primary_key_ptr: String,
}

#[derive(Debug, Clone)]
struct DatasetMetadata {
    root: PathBuf,
    schema: SchemaConfig,
}

#[derive(Debug, Clone)]
struct AppConfig {
    dataset: DatasetMetadata,
    validate: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct RecordMetadata {
    key: String,
    json_bytes: Vec<u8>,
    source_path: PathBuf,
    line_no: u64,
    mtime: SystemTime,
    size: u64,
}

#[derive(Debug)]
enum LoaderError {
    MissingSchema { path: PathBuf },
    InvalidPrimaryKeyPointer { pointer: String },
    SchemaRead { path: PathBuf, source: io::Error },
    SchemaParse { path: PathBuf, source: serde_json::Error },
}

impl std::fmt::Display for LoaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoaderError::MissingSchema { path } => {
                write!(f, "missing schema file at {}", path.display())
            }
            LoaderError::InvalidPrimaryKeyPointer { pointer } => {
                write!(f, "invalid primary key pointer: {pointer}")
            }
            LoaderError::SchemaRead { path, source } => {
                write!(f, "failed to read schema file at {}: {source}", path.display())
            }
            LoaderError::SchemaParse { path, source } => {
                write!(
                    f,
                    "failed to parse schema JSON at {}: {source}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for LoaderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LoaderError::SchemaRead { source, .. } => Some(source),
            LoaderError::SchemaParse { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[repr(C)]
struct ConstRowVTab {
    base: ffi::sqlite3_vtab,
}

#[repr(C)]
struct ConstRowCursor {
    base: ffi::sqlite3_vtab_cursor,
    done: bool,
}

unsafe impl<'vtab> VTab<'vtab> for ConstRowVTab {
    type Aux = ();
    type Cursor = ConstRowCursor;

    fn connect(
        _db: &mut VTabConnection,
        _aux: Option<&Self::Aux>,
        _args: &[&[u8]],
    ) -> Result<(String, Self)> {
        let vtab = ConstRowVTab {
            base: ffi::sqlite3_vtab::default(),
        };
        Ok(("CREATE TABLE x(value TEXT)".to_owned(), vtab))
    }

    fn best_index(&self, _info: &mut IndexInfo) -> Result<()> {
        Ok(())
    }

    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        Ok(ConstRowCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            done: false,
        })
    }
}

impl CreateVTab<'_> for ConstRowVTab {
    const KIND: VTabKind = VTabKind::Default;
}

unsafe impl VTabCursor for ConstRowCursor {
    fn filter(&mut self, _idx_num: c_int, _idx_str: Option<&str>, _args: &Values<'_>) -> Result<()> {
        self.done = false;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.done = true;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.done
    }

    fn column(&self, ctx: &mut Context, i: c_int) -> Result<()> {
        match i {
            0 => ctx.set_result(&"constant"),
            _ => ctx.set_result(&Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(1)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_app_config()?;
    let db = setup_db(&config)?;

    let mut stmt = db.prepare("SELECT value FROM const_row;")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let value: String = row.get(0)?;
        println!("{value}");
    }

    Ok(())
}

fn load_app_config() -> Result<AppConfig, Box<dyn std::error::Error>> {
    let validate = parse_validate_flag()?;
    let dataset_root = env::current_dir()?;
    let dataset = load_dataset_metadata(&dataset_root)?;
    Ok(AppConfig { dataset, validate })
}

fn parse_validate_flag() -> Result<bool, Box<dyn std::error::Error>> {
    let mut validate = false;
    for arg in env::args().skip(1) {
        match arg.as_str() {
            "--validate" => validate = true,
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    Ok(validate)
}

fn load_dataset_metadata(dataset_root: &Path) -> Result<DatasetMetadata, LoaderError> {
    let schema = load_schema_config(dataset_root)?;
    Ok(DatasetMetadata {
        root: dataset_root.to_path_buf(),
        schema,
    })
}

fn load_schema_config(dataset_root: &Path) -> Result<SchemaConfig, LoaderError> {
    let schema_path = dataset_root.join(".schema.json");
    if !schema_path.is_file() {
        return Err(LoaderError::MissingSchema { path: schema_path });
    }

    let schema_bytes = fs::read(&schema_path)
        .map_err(|source| LoaderError::SchemaRead {
            path: schema_path.clone(),
            source,
        })?;
    let schema_json: Value =
        serde_json::from_slice(&schema_bytes).map_err(|source| LoaderError::SchemaParse {
            path: schema_path.clone(),
            source,
        })?;

    let primary_key_ptr = schema_json
        .get("x-primaryKey")
        .and_then(Value::as_str)
        .ok_or_else(|| LoaderError::InvalidPrimaryKeyPointer {
            pointer: "<missing>".to_string(),
        })?;
    validate_json_pointer(primary_key_ptr)?;

    Ok(SchemaConfig {
        path: schema_path,
        primary_key_ptr: primary_key_ptr.to_string(),
    })
}

fn validate_json_pointer(pointer: &str) -> Result<(), LoaderError> {
    if is_valid_json_pointer(pointer) {
        Ok(())
    } else {
        Err(LoaderError::InvalidPrimaryKeyPointer {
            pointer: pointer.to_string(),
        })
    }
}

fn is_valid_json_pointer(pointer: &str) -> bool {
    if pointer.is_empty() {
        return true;
    }

    if !pointer.starts_with('/') {
        return false;
    }

    let bytes = pointer.as_bytes();
    let mut i = 1;
    while i < bytes.len() {
        if bytes[i] == b'~' {
            if i + 1 >= bytes.len() {
                return false;
            }
            let next = bytes[i + 1];
            if next != b'0' && next != b'1' {
                return false;
            }
            i += 2;
        } else {
            i += 1;
        }
    }

    true
}

fn setup_db(config: &AppConfig) -> Result<Connection> {
    let _ = (
        &config.dataset.root,
        &config.dataset.schema.path,
        &config.dataset.schema.primary_key_ptr,
        config.validate,
    );
    let db = Connection::open_in_memory()?;
    let aux: Option<()> = None;
    db.create_module("constrow", read_only_module::<ConstRowVTab>(), aux)?;
    db.execute_batch("CREATE VIRTUAL TABLE const_row USING constrow;")?;
    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::{
        load_schema_config, setup_db, AppConfig, DatasetMetadata, LoaderError, SchemaConfig,
    };
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let mut path = env::temp_dir();
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time since epoch")
                .as_nanos();
            path.push(format!("jsondex-test-{}-{stamp}", process::id()));
            fs::create_dir(&path).expect("create temp dir");
            Self { path }
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn constrow_returns_single_constant_row() {
        let config = AppConfig {
            validate: false,
            dataset: DatasetMetadata {
                root: PathBuf::from("testdata"),
                schema: SchemaConfig {
                    path: PathBuf::from("testdata/.schema.json"),
                    primary_key_ptr: "/id".to_string(),
                },
            },
        };
        let db = setup_db(&config).expect("db setup");
        let mut stmt = db
            .prepare("SELECT value FROM const_row")
            .expect("prepare query");
        let mut rows = stmt.query([]).expect("query rows");

        let row = rows.next().expect("row fetch").expect("row exists");
        let value: String = row.get(0).expect("value");
        assert_eq!(value, "constant");

        let next = rows.next().expect("row fetch");
        assert!(next.is_none());
    }

    #[test]
    fn load_schema_config_reads_primary_key() {
        let dir = TestDir::new();
        let schema_path = dir.path.join(".schema.json");
        fs::write(&schema_path, r#"{"x-primaryKey":"/id"}"#).expect("write schema");

        let schema = load_schema_config(&dir.path).expect("load schema");
        assert_eq!(schema.primary_key_ptr, "/id");
        assert_eq!(schema.path, schema_path);
    }

    #[test]
    fn load_schema_config_errors_on_missing_schema() {
        let dir = TestDir::new();
        let err = load_schema_config(&dir.path).expect_err("missing schema");
        assert!(matches!(err, LoaderError::MissingSchema { .. }));
    }

    #[test]
    fn load_schema_config_errors_on_invalid_pointer() {
        let dir = TestDir::new();
        let schema_path = dir.path.join(".schema.json");
        fs::write(&schema_path, r#"{"x-primaryKey":"id"}"#).expect("write schema");

        let err = load_schema_config(&dir.path).expect_err("invalid pointer");
        assert!(matches!(err, LoaderError::InvalidPrimaryKeyPointer { .. }));
    }
}
