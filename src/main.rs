use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::os::raw::c_int;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonschema::JSONSchema;
use rusqlite::ffi;
use rusqlite::types::Null;
use rusqlite::vtab::{
    read_only_module, Context, CreateVTab, IndexConstraintOp, IndexFlags, IndexInfo, VTab,
    VTabConfig, VTabConnection, VTabCursor, VTabKind, Values,
};
use rusqlite::{Connection, Error, Result};
use serde_json::Value;

#[derive(Debug, Clone)]
struct SchemaConfig {
    path: PathBuf,
    primary_key_ptr: String,
    schema_json: Value,
}

#[derive(Debug, Clone)]
struct DatasetMetadata {
    root: PathBuf,
    schema: SchemaConfig,
}

#[derive(Debug, Clone)]
struct Dataset {
    metadata: DatasetMetadata,
    records: Vec<RecordMetadata>,
    index: HashMap<String, usize>,
}

#[derive(Debug, Clone)]
struct AppConfig {
    dataset: Arc<Dataset>,
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
struct FileSnapshot {
    bytes: Vec<u8>,
    mtime: SystemTime,
    size: u64,
}

const FILE_READ_RETRIES: usize = 3;
const FILE_READ_DELAY_MS: u64 = 10;

#[derive(Debug)]
enum LoaderError {
    MissingSchema { path: PathBuf },
    InvalidPrimaryKeyPointer { pointer: String },
    SchemaRead { path: PathBuf, source: io::Error },
    SchemaParse { path: PathBuf, source: serde_json::Error },
    SchemaCompile { path: PathBuf, message: String },
    DatasetScan { path: PathBuf, source: io::Error },
    FileMetadata { path: PathBuf, source: io::Error },
    FileRead { path: PathBuf, source: io::Error },
    FileChanged { path: PathBuf },
    JsonParse {
        path: PathBuf,
        line_no: u64,
        source: serde_json::Error,
    },
    SchemaValidation {
        path: PathBuf,
        line_no: u64,
        message: String,
    },
    DuplicateKey {
        key: String,
        path: PathBuf,
        line_no: u64,
        existing_path: PathBuf,
        existing_line: u64,
    },
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
            LoaderError::SchemaCompile { path, message } => {
                write!(
                    f,
                    "failed to compile schema at {}: {message}",
                    path.display()
                )
            }
            LoaderError::DatasetScan { path, source } => {
                write!(
                    f,
                    "failed to scan dataset directory {}: {source}",
                    path.display()
                )
            }
            LoaderError::FileMetadata { path, source } => {
                write!(
                    f,
                    "failed to read file metadata at {}: {source}",
                    path.display()
                )
            }
            LoaderError::FileRead { path, source } => {
                write!(f, "failed to read data file at {}: {source}", path.display())
            }
            LoaderError::FileChanged { path } => {
                write!(f, "data file changed during read: {}", path.display())
            }
            LoaderError::JsonParse {
                path,
                line_no,
                source,
            } => {
                write!(
                    f,
                    "failed to parse JSON at {}:{}: {source}",
                    path.display(),
                    line_no
                )
            }
            LoaderError::SchemaValidation {
                path,
                line_no,
                message,
            } => {
                write!(
                    f,
                    "schema validation failed at {}:{}: {message}",
                    path.display(),
                    line_no
                )
            }
            LoaderError::DuplicateKey {
                key,
                path,
                line_no,
                existing_path,
                existing_line,
            } => {
                write!(
                    f,
                    "duplicate key \"{key}\" at {}:{} (already defined at {}:{})",
                    path.display(),
                    line_no,
                    existing_path.display(),
                    existing_line
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
            LoaderError::DatasetScan { source, .. } => Some(source),
            LoaderError::FileMetadata { source, .. } => Some(source),
            LoaderError::FileRead { source, .. } => Some(source),
            LoaderError::JsonParse { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[repr(C)]
struct DatasetVTab {
    base: ffi::sqlite3_vtab,
    dataset: Arc<Dataset>,
}

#[repr(C)]
struct DatasetCursor {
    base: ffi::sqlite3_vtab_cursor,
    dataset: Arc<Dataset>,
    row_index: usize,
    use_key: bool,
}

const IDX_SCAN: c_int = 0;
const IDX_KEY_EQ: c_int = 1;

const COL_KEY: c_int = 0;
const COL_JSON: c_int = 1;
const COL_SOURCE_PATH: c_int = 2;
const COL_LINE_NO: c_int = 3;
const COL_MTIME: c_int = 4;
const COL_SIZE: c_int = 5;

unsafe impl<'vtab> VTab<'vtab> for DatasetVTab {
    type Aux = Arc<Dataset>;
    type Cursor = DatasetCursor;

    fn connect(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        _args: &[&[u8]],
    ) -> Result<(String, Self)> {
        db.config(VTabConfig::ConstraintSupport)?;
        let dataset = aux
            .cloned()
            .ok_or_else(|| Error::ModuleError("dataset snapshot missing".to_string()))?;
        let vtab = DatasetVTab {
            base: ffi::sqlite3_vtab::default(),
            dataset,
        };
        Ok((
            "CREATE TABLE x(key TEXT, json TEXT, source_path TEXT, line_no INTEGER, mtime INTEGER, size INTEGER)"
                .to_owned(),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        for (constraint, mut usage) in info.constraints_and_usages() {
            if constraint.is_usable()
                && constraint.column() == COL_KEY
                && constraint.operator() == IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ
            {
                usage.set_argv_index(1);
                usage.set_omit(true);
                info.set_idx_num(IDX_KEY_EQ);
                info.set_estimated_cost(1.0);
                info.set_estimated_rows(1);
                info.set_idx_flags(IndexFlags::SQLITE_INDEX_SCAN_UNIQUE);
                return Ok(());
            }
        }

        let rows = self.dataset.records.len() as i64;
        info.set_idx_num(IDX_SCAN);
        info.set_estimated_cost(rows as f64);
        info.set_estimated_rows(rows);
        Ok(())
    }

    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        Ok(DatasetCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            dataset: Arc::clone(&self.dataset),
            row_index: 0,
            use_key: false,
        })
    }
}

impl CreateVTab<'_> for DatasetVTab {
    const KIND: VTabKind = VTabKind::Default;
}

unsafe impl VTabCursor for DatasetCursor {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Values<'_>) -> Result<()> {
        self.use_key = idx_num == IDX_KEY_EQ;
        if self.use_key {
            if args.is_empty() {
                self.row_index = self.dataset.records.len();
                return Ok(());
            }
            let key: String = args.get(0)?;
            self.row_index = self
                .dataset
                .index
                .get(&key)
                .copied()
                .unwrap_or(self.dataset.records.len());
        } else {
            self.row_index = 0;
        }
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        if self.use_key {
            self.row_index = self.dataset.records.len();
        } else {
            self.row_index = self.row_index.saturating_add(1);
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        self.row_index >= self.dataset.records.len()
    }

    fn column(&self, ctx: &mut Context, i: c_int) -> Result<()> {
        let record = self.dataset.records.get(self.row_index).ok_or_else(|| {
            Error::ModuleError("cursor out of bounds for dataset".to_string())
        })?;
        match i {
            COL_KEY => ctx.set_result(&record.key),
            COL_JSON => {
                let json = String::from_utf8_lossy(&record.json_bytes).into_owned();
                ctx.set_result(&json)
            }
            COL_SOURCE_PATH => {
                let path = record.source_path.to_string_lossy().into_owned();
                ctx.set_result(&path)
            }
            COL_LINE_NO => ctx.set_result(&clamp_u64_to_i64(record.line_no)),
            COL_MTIME => ctx.set_result(&system_time_to_unix_secs(record.mtime)),
            COL_SIZE => ctx.set_result(&clamp_u64_to_i64(record.size)),
            _ => ctx.set_result(&Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        let record = self.dataset.records.get(self.row_index).ok_or_else(|| {
            Error::ModuleError("cursor out of bounds for dataset".to_string())
        })?;
        Ok(stable_rowid(&record.key))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_app_config()?;
    let db = setup_db(&config)?;

    let mut stmt = db.prepare("SELECT key, json FROM dataset;")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let key: String = row.get(0)?;
        let json: String = row.get(1)?;
        println!("{key}\t{json}");
    }

    Ok(())
}

fn load_app_config() -> Result<AppConfig, Box<dyn std::error::Error>> {
    let validate = parse_validate_flag()?;
    let dataset_root = env::current_dir()?;
    let dataset = load_dataset(&dataset_root, validate)?;
    Ok(AppConfig {
        dataset: Arc::new(dataset),
        validate,
    })
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

fn load_dataset(dataset_root: &Path, validate: bool) -> Result<Dataset, LoaderError> {
    let metadata = load_dataset_metadata(dataset_root)?;
    let validator = if validate {
        Some(build_validator(&metadata.schema)?)
    } else {
        None
    };
    let files = collect_dataset_files(dataset_root)?;
    let mut records = Vec::new();
    let mut index = HashMap::new();

    for path in files {
        let snapshot = read_file_stable(&path)?;
        let relative_path = relative_path_string(dataset_root, &path);
        let extension = path.extension().and_then(|ext| ext.to_str());
        match extension {
            Some(ext) if ext.eq_ignore_ascii_case("jsonl") => {
                load_jsonl_records(
                    &metadata.schema,
                    &path,
                    &relative_path,
                    snapshot,
                    validator.as_ref(),
                    &mut records,
                    &mut index,
                )?;
            }
            Some(ext) if ext.eq_ignore_ascii_case("json") => {
                load_json_record(
                    &metadata.schema,
                    &path,
                    &relative_path,
                    snapshot,
                    validator.as_ref(),
                    &mut records,
                    &mut index,
                )?;
            }
            _ => {}
        }
    }

    Ok(Dataset {
        metadata,
        records,
        index,
    })
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
        schema_json,
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

fn build_validator(schema: &SchemaConfig) -> Result<JSONSchema, LoaderError> {
    JSONSchema::compile(&schema.schema_json).map_err(|err| LoaderError::SchemaCompile {
        path: schema.path.clone(),
        message: err.to_string(),
    })
}

fn collect_dataset_files(dataset_root: &Path) -> Result<Vec<PathBuf>, LoaderError> {
    let mut files = Vec::new();
    collect_dataset_files_inner(dataset_root, &mut files)?;
    files.sort_by(|left, right| {
        relative_path_string(dataset_root, left).cmp(&relative_path_string(dataset_root, right))
    });
    Ok(files)
}

fn collect_dataset_files_inner(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), LoaderError> {
    let entries = fs::read_dir(dir).map_err(|source| LoaderError::DatasetScan {
        path: dir.to_path_buf(),
        source,
    })?;
    let mut entries: Vec<_> = entries
        .collect::<Result<_, _>>()
        .map_err(|source| LoaderError::DatasetScan {
            path: dir.to_path_buf(),
            source,
        })?;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|source| LoaderError::DatasetScan {
                path: path.clone(),
                source,
            })?;
        if file_type.is_dir() {
            collect_dataset_files_inner(&path, files)?;
        } else if file_type.is_file() && is_dataset_file(&path) {
            files.push(path);
        }
    }
    Ok(())
}

fn is_dataset_file(path: &Path) -> bool {
    let file_name = path.file_name().and_then(|name| name.to_str());
    if file_name == Some(".schema.json") {
        return false;
    }
    match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) if ext.eq_ignore_ascii_case("json") => true,
        Some(ext) if ext.eq_ignore_ascii_case("jsonl") => true,
        _ => false,
    }
}

fn relative_path_string(root: &Path, path: &Path) -> String {
    let relative = path.strip_prefix(root).unwrap_or(path);
    let mut normalized = String::new();
    for (index, component) in relative.components().enumerate() {
        if index > 0 {
            normalized.push('/');
        }
        normalized.push_str(&component.as_os_str().to_string_lossy());
    }
    normalized
}

fn read_file_stable(path: &Path) -> Result<FileSnapshot, LoaderError> {
    for attempt in 0..FILE_READ_RETRIES {
        let (before_mtime, before_size) = read_file_metadata(path)?;
        let bytes = fs::read(path).map_err(|source| LoaderError::FileRead {
            path: path.to_path_buf(),
            source,
        })?;
        let (after_mtime, after_size) = read_file_metadata(path)?;
        let bytes_len = bytes.len() as u64;

        if before_size == after_size && before_mtime == after_mtime && bytes_len == after_size {
            return Ok(FileSnapshot {
                bytes,
                mtime: after_mtime,
                size: after_size,
            });
        }

        if attempt + 1 < FILE_READ_RETRIES {
            thread::sleep(Duration::from_millis(FILE_READ_DELAY_MS));
        }
    }

    Err(LoaderError::FileChanged {
        path: path.to_path_buf(),
    })
}

fn read_file_metadata(path: &Path) -> Result<(SystemTime, u64), LoaderError> {
    let metadata = fs::metadata(path).map_err(|source| LoaderError::FileMetadata {
        path: path.to_path_buf(),
        source,
    })?;
    let modified = metadata.modified().map_err(|source| LoaderError::FileMetadata {
        path: path.to_path_buf(),
        source,
    })?;
    Ok((modified, metadata.len()))
}

fn load_json_record(
    schema: &SchemaConfig,
    path: &Path,
    relative_path: &str,
    snapshot: FileSnapshot,
    validator: Option<&JSONSchema>,
    records: &mut Vec<RecordMetadata>,
    index: &mut HashMap<String, usize>,
) -> Result<(), LoaderError> {
    let line_no = 1;
    let value: Value =
        serde_json::from_slice(&snapshot.bytes).map_err(|source| LoaderError::JsonParse {
            path: path.to_path_buf(),
            line_no,
            source,
        })?;
    validate_record(validator, &value, path, line_no)?;
    let key = record_key(&value, &schema.primary_key_ptr, relative_path);
    let record = RecordMetadata {
        key,
        json_bytes: snapshot.bytes,
        source_path: path.to_path_buf(),
        line_no,
        mtime: snapshot.mtime,
        size: snapshot.size,
    };
    insert_record(record, index, records)?;
    Ok(())
}

fn load_jsonl_records(
    schema: &SchemaConfig,
    path: &Path,
    relative_path: &str,
    snapshot: FileSnapshot,
    validator: Option<&JSONSchema>,
    records: &mut Vec<RecordMetadata>,
    index: &mut HashMap<String, usize>,
) -> Result<(), LoaderError> {
    let mut lines: Vec<&[u8]> = snapshot.bytes.split(|byte| *byte == b'\n').collect();
    let has_trailing_newline = snapshot.bytes.last().map(|byte| *byte == b'\n').unwrap_or(false);
    if has_trailing_newline {
        if lines.last().is_some_and(|line| line.is_empty()) {
            lines.pop();
        }
    } else if !lines.is_empty() {
        lines.pop();
    }

    for (index_line, line) in lines.iter().enumerate() {
        let line_no = index_line as u64 + 1;
        let trimmed = trim_cr(line);
        let value: Value =
            serde_json::from_slice(trimmed).map_err(|source| LoaderError::JsonParse {
                path: path.to_path_buf(),
                line_no,
                source,
            })?;
        validate_record(validator, &value, path, line_no)?;
        let fallback_key = format!("{relative_path}#{line_no}");
        let key = record_key(&value, &schema.primary_key_ptr, &fallback_key);
        let record = RecordMetadata {
            key,
            json_bytes: trimmed.to_vec(),
            source_path: path.to_path_buf(),
            line_no,
            mtime: snapshot.mtime,
            size: snapshot.size,
        };
        insert_record(record, index, records)?;
    }
    Ok(())
}

fn trim_cr(line: &[u8]) -> &[u8] {
    if line.ends_with(b"\r") {
        &line[..line.len() - 1]
    } else {
        line
    }
}

fn validate_record(
    validator: Option<&JSONSchema>,
    value: &Value,
    path: &Path,
    line_no: u64,
) -> Result<(), LoaderError> {
    if let Some(validator) = validator {
        if let Err(errors) = validator.validate(value) {
            if let Some(error) = errors.into_iter().next() {
                return Err(LoaderError::SchemaValidation {
                    path: path.to_path_buf(),
                    line_no,
                    message: error.to_string(),
                });
            }
            return Err(LoaderError::SchemaValidation {
                path: path.to_path_buf(),
                line_no,
                message: "schema validation failed".to_string(),
            });
        }
    }
    Ok(())
}

fn record_key(value: &Value, primary_key_ptr: &str, fallback: &str) -> String {
    match value.pointer(primary_key_ptr) {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Null) | None => fallback.to_string(),
        Some(value) => value.to_string(),
    }
}

fn insert_record(
    record: RecordMetadata,
    index: &mut HashMap<String, usize>,
    records: &mut Vec<RecordMetadata>,
) -> Result<(), LoaderError> {
    let key = record.key.clone();
    match index.entry(key.clone()) {
        Entry::Vacant(entry) => {
            let next_index = records.len();
            entry.insert(next_index);
            records.push(record);
            Ok(())
        }
        Entry::Occupied(entry) => {
            let existing = &records[*entry.get()];
            Err(LoaderError::DuplicateKey {
                key,
                path: record.source_path.clone(),
                line_no: record.line_no,
                existing_path: existing.source_path.clone(),
                existing_line: existing.line_no,
            })
        }
    }
}

fn clamp_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn system_time_to_unix_secs(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => clamp_u64_to_i64(duration.as_secs()),
        Err(_) => 0,
    }
}

fn stable_rowid(key: &str) -> i64 {
    let mut hash: u64 = 14695981039346656037;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(1099511628211);
    }
    (hash & 0x7fff_ffff_ffff_ffff) as i64
}

fn setup_db(config: &AppConfig) -> Result<Connection> {
    let _ = (
        &config.dataset.metadata.root,
        &config.dataset.metadata.schema.path,
        &config.dataset.metadata.schema.primary_key_ptr,
        config.dataset.records.len(),
        config.validate,
    );
    let db = Connection::open_in_memory()?;
    let aux = Some(Arc::clone(&config.dataset));
    db.create_module("jsondex", read_only_module::<DatasetVTab>(), aux)?;
    db.execute_batch("CREATE VIRTUAL TABLE dataset USING jsondex;")?;
    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::{
        load_schema_config, setup_db, AppConfig, Dataset, DatasetMetadata, LoaderError,
        RecordMetadata, SchemaConfig,
    };
    use serde_json::json;
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::process;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    fn dataset_vtab_returns_record() {
        let json_text = r#"{"id":1}"#;
        let source_path = PathBuf::from("testdata/one.json");
        let source_path_text = source_path.to_string_lossy().into_owned();
        let mtime_secs: u64 = 42;
        let dataset = Dataset {
            metadata: DatasetMetadata {
                root: PathBuf::from("testdata"),
                schema: SchemaConfig {
                    path: PathBuf::from("testdata/.schema.json"),
                    primary_key_ptr: "/id".to_string(),
                    schema_json: json!({"x-primaryKey": "/id"}),
                },
            },
            records: vec![RecordMetadata {
                key: "alpha".to_string(),
                json_bytes: json_text.as_bytes().to_vec(),
                source_path,
                line_no: 1,
                mtime: UNIX_EPOCH + Duration::from_secs(mtime_secs),
                size: json_text.len() as u64,
            }],
            index: HashMap::from([("alpha".to_string(), 0)]),
        };
        let config = AppConfig {
            validate: false,
            dataset: Arc::new(dataset),
        };
        let db = setup_db(&config).expect("db setup");
        let mut stmt = db
            .prepare("SELECT key, json, source_path, line_no, mtime, size FROM dataset")
            .expect("prepare query");
        let mut rows = stmt.query([]).expect("query rows");

        let row = rows.next().expect("row fetch").expect("row exists");
        let key: String = row.get(0).expect("key");
        let json: String = row.get(1).expect("json");
        let source: String = row.get(2).expect("source");
        let line_no: i64 = row.get(3).expect("line");
        let mtime: i64 = row.get(4).expect("mtime");
        let size: i64 = row.get(5).expect("size");
        assert_eq!(key, "alpha");
        assert_eq!(json, json_text);
        assert_eq!(source, source_path_text);
        assert_eq!(line_no, 1);
        assert_eq!(mtime, mtime_secs as i64);
        assert_eq!(size, json_text.len() as i64);

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
        assert_eq!(schema.schema_json, json!({"x-primaryKey":"/id"}));
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
