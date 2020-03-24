use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use std::ops::Deref;

use std::convert;
use std::result;

use rusqlite::*;

use thread_local::ThreadLocal;

static COUNTER: AtomicU64 = AtomicU64::new(0u64);

pub fn open_shared(name: &str) -> Result<Connection> {
    let uri = format!("file:{}?mode=memory&cache=shared", name);
    Connection::open(uri)
}

pub fn new_shared() -> Result<Connection> {
    open_shared(&format!(
        "shared_{}",
        COUNTER.fetch_add(1u64, Ordering::AcqRel)
    ))
}

pub struct SyncSqliteConnection {
    connection: ThreadLocal<Connection>,
    name: String,
}

impl SyncSqliteConnection {
    pub fn new() -> Result<Self> {
        let name = format!("shared_{}", COUNTER.fetch_add(1u64, Ordering::AcqRel));

        let this = SyncSqliteConnection {
            connection: ThreadLocal::new(),
            name: name,
        };

        this.try_get()?;
        Result::Ok(this)
    }

    pub fn open(name: String) -> Result<Self> {
        let this = SyncSqliteConnection {
            connection: ThreadLocal::new(),
            name: name,
        };

        this.try_get()?;
        Result::Ok(this)
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    fn try_get(&self) -> Result<&Connection> {
        self.connection.get_or_try(|| open_shared(&self.name()))
    }

    pub fn force(&self) -> &Connection {
        self.try_get()
            .expect("ERROR: Creating the connection to the sqlite in memory database has failed!")
    }

    pub fn prepare(&self, sql: &str) -> Result<SyncStatement<'_>> {
        SyncStatement::new(self, sql.to_owned())
    }
}

impl Deref for SyncSqliteConnection {
    type Target = Connection;
    fn deref(&self) -> &Self::Target {
        self.force()
    }
}

impl Clone for SyncSqliteConnection {
    fn clone(&self) -> Self {
        SyncSqliteConnection::open(self.name().clone())
            .expect("ERROR: opening the sqlite database has failed!")
    }

    fn clone_from(&mut self, source: &Self) {
        self.name = source.name().clone();
        self.connection.clear();
    }
}

struct SendStatement<'a>(Statement<'a>);

unsafe impl<'a> Send for SendStatement<'a> {}

pub struct SyncStatement<'conn> {
    conn: &'conn SyncSqliteConnection,
    stmt: ThreadLocal<SendStatement<'conn>>,
    sql: String,
}

impl<'conn> SyncStatement<'conn> {
    fn new(conn: &'conn SyncSqliteConnection, sql: String) -> Result<SyncStatement<'conn>> {
        let this = SyncStatement {
            conn: conn,
            stmt: ThreadLocal::new(),
            sql: sql,
        };

        this.try_get()?;
        Result::Ok(this)
    }

    fn try_get(&self) -> Result<&Statement<'_>> {
        self.stmt
            .get_or_try(|| {
                self.conn
                    .try_get()
                    .and_then(|conn| conn.prepare(&self.sql).map(|stmt| SendStatement(stmt)))
            })
            .map(|ss| &ss.0)
    }

    pub fn execute<P>(&self, params: P) -> Result<usize>
    where
        P: IntoIterator,
        P::Item: ToSql,
    {
        let statement = self.try_get()?;
        unsafe { &mut *(statement as *const _ as *mut Statement) }.execute(params)
    }

    pub fn execute_named(&self, params: &[(&str, &dyn ToSql)]) -> Result<usize> {
        let statement = self.try_get()?;
        unsafe { &mut *(statement as *const _ as *mut Statement) }.execute_named(params)
    }

    pub fn exists<P>(&self, params: P) -> Result<bool>
    where
        P: IntoIterator,
        P::Item: ToSql,
    {
        let statement = self.try_get()?;
        unsafe { &mut *(statement as *const _ as *mut Statement) }.exists(params)
    }

    pub fn insert<P>(&self, params: P) -> Result<i64>
    where
        P: IntoIterator,
        P::Item: ToSql,
    {
        let statement = self.try_get()?;
        unsafe { &mut *(statement as *const _ as *mut Statement) }.insert(params)
    }

    pub fn query<P>(&self, params: P) -> Result<Rows<'_>>
    where
        P: IntoIterator,
        P::Item: ToSql,
    {
        let statement = self.try_get()?;
        unsafe { &mut *(statement as *const _ as *mut Statement) }.query(params)
    }

    pub fn query_named(&self, params: &[(&str, &dyn ToSql)]) -> Result<Rows<'_>> {
        let statement = self.try_get()?;
        unsafe { &mut *(statement as *const _ as *mut Statement) }.query_named(params)
    }

    pub fn query_map<T, P, F>(&self, params: P, f: F) -> Result<MappedRows<'_, F>>
    where
        P: IntoIterator,
        P::Item: ToSql,
        F: FnMut(&Row<'_>) -> Result<T>,
    {
        let statement = self.try_get()?;
        unsafe { &mut *(statement as *const _ as *mut Statement) }.query_map(params, f)
    }

    pub fn query_map_named<T, F>(
        &self,
        params: &[(&str, &dyn ToSql)],
        f: F,
    ) -> Result<MappedRows<'_, F>>
    where
        F: FnMut(&Row<'_>) -> Result<T>,
    {
        let statement = self.try_get()?;
        unsafe { &mut *(statement as *const _ as *mut Statement) }.query_map_named(params, f)
    }

    pub fn query_and_then<T, E, P, F>(&self, params: P, f: F) -> Result<AndThenRows<'_, F>>
    where
        P: IntoIterator,
        P::Item: ToSql,
        E: convert::From<Error>,
        F: FnMut(&Row<'_>) -> result::Result<T, E>,
    {
        let statement = self.try_get()?;
        unsafe { &mut *(statement as *const _ as *mut Statement) }.query_and_then(params, f)
    }

    pub fn query_and_then_named<T, E, F>(
        &self,
        params: &[(&str, &dyn ToSql)],
        f: F,
    ) -> Result<AndThenRows<'_, F>>
    where
        E: convert::From<Error>,
        F: FnMut(&Row<'_>) -> result::Result<T, E>,
    {
        let statement = self.try_get()?;
        unsafe { &mut *(statement as *const _ as *mut Statement) }.query_and_then_named(params, f)
    }

    pub fn query_row<T, P, F>(&self, params: P, f: F) -> Result<T>
    where
        P: IntoIterator,
        P::Item: ToSql,
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        let statement = self.try_get()?;
        unsafe { &mut *(statement as *const _ as *mut Statement) }.query_row(params, f)
    }

    pub fn query_row_named<T, F>(&self, params: &[(&str, &dyn ToSql)], f: F) -> Result<T>
    where
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        let statement = self.try_get()?;
        unsafe { &mut *(statement as *const _ as *mut Statement) }.query_row_named(params, f)
    }

    pub fn parameter_index(&self, name: &str) -> Result<Option<usize>> {
        let statement = self.try_get()?;
        unsafe { &mut *(statement as *const _ as *mut Statement) }.parameter_index(name)
    }

    pub fn force(&self) -> &Statement<'_> {
        self.try_get()
            .expect("ERROR: Building the prepared statement has failed!")
    }

    pub fn deref(&self) -> &Statement<'_> {
        self.force()
    }
}

impl<'conn> Clone for SyncStatement<'conn> {
    fn clone(&self) -> Self {
        SyncStatement::new(self.conn, self.sql.clone())
            .expect("ERROR: creating the sqlitet prepared statement has failed!")
    }

    fn clone_from(&mut self, source: &Self) {
        self.conn = source.conn;
        self.sql = source.sql.clone();
        self.stmt.clear();
    }
}

mod test {

    #[test]
    fn testnew() {
        let _ignore = crate::SyncSqliteConnection::new();
    }

    #[test]
    fn testnewrealconnection() {
        let _connection = crate::SyncSqliteConnection::new().unwrap();
    }

    #[test]
    fn test_open() {
        let dummy = crate::SyncSqliteConnection::new().unwrap();

        let c1 = crate::SyncSqliteConnection::new().unwrap();

        let c2 = crate::SyncSqliteConnection::open(c1.name().clone()).unwrap();

        assert_eq!(c1.name(), c2.name());
        assert_ne!(dummy.name(), c1.name());
    }

    #[test]
    fn test_clone() {
        let c1 = crate::SyncSqliteConnection::new().unwrap();

        let c2 = c1.clone();
        assert_eq!(c1.name(), c2.name());
    }
}
