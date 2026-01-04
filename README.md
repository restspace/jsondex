# jsondex

Rust single-binary demo that embeds SQLite and registers a custom virtual table.
The virtual table exposes one constant row.

## Build

```
cargo build
```

## Run

```
cargo run
```

The demo expects a `.schema.json` file in the working directory with
`x-primaryKey` set to a JSON Pointer string. The repository includes a minimal
sample schema.

Expected output:

```
constant
```

You can also pass `--validate` (default off) to exercise the validation flag
plumbing:

```
cargo run -- --validate
```
