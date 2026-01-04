## Project Overview
Project location: `C:\dev\jsondex`.
`jsondex` is a Rust single-binary demo that embeds SQLite and registers a custom
read-only virtual table. The virtual table (`const_row`) returns exactly one row
with a constant value.

## Layout
- `C:\dev\jsondex\Cargo.toml`: Rust package manifest (uses `rusqlite` with `bundled` + `vtab`).
- `C:\dev\jsondex\src\main.rs`: Virtual table implementation, DB setup, and a small CLI run path.
- `C:\dev\jsondex\README.md`: Build/run notes.
- `C:\dev\jsondex\.gitignore`: Rust `target/` ignore.

## Key Behavior
- On startup, the app registers the `constrow` module and creates
  `const_row` as a virtual table.
- Query `SELECT value FROM const_row;` returns one row: `constant`.

## Build and Test
- Build: `cargo build` (writes to `C:\dev\jsondex\target\`).
- Run: `cargo run` (prints `constant`).
- Test: `cargo test` (verifies the virtual table returns one row).

## Notes
- The Rust toolchain is installed via rustup; executables live in
  `C:\Users\james\.cargo\bin`.
- On this machine, build/test may require elevated permissions to create
  `C:\dev\jsondex\target\`.

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
