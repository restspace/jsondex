use std::os::raw::c_int;

use rusqlite::ffi;
use rusqlite::types::Null;
use rusqlite::vtab::{
    read_only_module, Context, CreateVTab, IndexInfo, VTab, VTabConnection, VTabCursor, VTabKind,
    Values,
};
use rusqlite::{Connection, Result};

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

fn main() -> Result<()> {
    let db = setup_db()?;

    let mut stmt = db.prepare("SELECT value FROM const_row;")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let value: String = row.get(0)?;
        println!("{value}");
    }

    Ok(())
}

fn setup_db() -> Result<Connection> {
    let db = Connection::open_in_memory()?;
    let aux: Option<()> = None;
    db.create_module("constrow", read_only_module::<ConstRowVTab>(), aux)?;
    db.execute_batch("CREATE VIRTUAL TABLE const_row USING constrow;")?;
    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::setup_db;

    #[test]
    fn constrow_returns_single_constant_row() {
        let db = setup_db().expect("db setup");
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
}
