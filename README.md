# jsondex

Rust single-binary demo that embeds SQLite and registers a custom virtual table.
The virtual table exposes an in-memory dataset snapshot with columns for key,
json, source_path, line_no, mtime, and size.

## Build

```
cargo build
```

## Run

```
cargo run
```

The demo expects a `.schema.json` file in the working directory with
`x-primaryKey` set to a JSON Pointer string. Any `.json` or `.jsonl` files
alongside the schema are loaded into the dataset.

Output depends on the dataset contents and prints `key` and `json` for each
record (empty output means no records were found).

You can also pass `--validate` (default off) to exercise the validation flag
plumbing:

```
cargo run -- --validate
```
