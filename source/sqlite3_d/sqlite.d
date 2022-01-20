module sqlite3_d.sqlite;

import std.conv : to;
import
	std.exception,
	std.string,
	std.meta,
	std.typecons,
	etc.c.sqlite3,
	sqlite3_d.sqlbuilder,
	sqlite3_d.utils;

version (Windows) {
	// manually link in dub.json
	//pragma(lib, "sqlite3");
} else version (linux) {
	pragma(lib, "sqlite3");
} else version (OSX) {
	pragma(lib, "sqlite3");
} else version (Posix) {
	pragma(lib, "libsqlite3");
} else {
	pragma(msg, "You need to manually link in the SQLite library.");
}

class DBException : Exception {
	mixin ExceptionThis;
};

class SQLiteException : DBException {
	mixin ExceptionThis;
}

/// Setup code for tests
version(unittest) package template TEST(string dbname)
{
	SQLite3 db = () {
		tryRemove(dbname ~ ".db");
		return new SQLite3(dbname ~ ".db");
	}();
}

struct Statement
{
	~this() { close(); }
	sqlite3_stmt* s;
	alias s this;
	void close() {
		sqlite3_finalize(s); s = null;
	}
	bool closed() const { return s is null; }
	T opCast(T : bool)() const { return !closed(); }
}

/// Represents a sqlite3 statement
struct Query
{
	int lastCode;

	/// Construct a query from the string 'sql' into database 'db'
	this(ARGS...)(sqlite3* db, string sql, ARGS args)
	in(db)
	in(sql.length) {
		lastCode = -1;
		sqlite3_stmt* s;
		int rc = sqlite3_prepare_v2(db, sql.toz, -1, &s, null);
		checkError("Prepare failed: ", rc);
		stmt = s;
		this.db = db;
		set(args);
	}

	~this() { sqlite3_close(db); db = null; }

	bool closed() const { return db is null; }
	T opCast(T : bool)() const { return !closed(); }

	@property int changes() in(db) { return sqlite3_changes(db); }

private:
	sqlite3* db;
	RefCounted!Statement stmt;

	int bindArg(S)(int pos, S arg) if(isSomeString!S)
	{
		static if(size_t.sizeof > 4)
			return sqlite3_bind_text64(stmt, pos, arg.ptr, arg.length, null, SQLITE_UTF8);
		else
			return sqlite3_bind_text(stmt, pos, arg.ptr, cast(int)arg.length, null);
	}

	int bindArg(int pos, double arg)
	{
		return sqlite3_bind_double(stmt, pos, arg);
	}

	int bindArg(T)(int pos, T arg) if(isIntegral!T && T.sizeof <= 4) {
		return sqlite3_bind_int(stmt, pos, arg);
	}

	int bindArg(T)(int pos, T arg) if(isIntegral!T && T.sizeof > 4) {
		return sqlite3_bind_int64(stmt, pos, arg);
	}

	int bindArg(int pos, void[] arg)
	{
		static if(size_t.sizeof > 4)
			return sqlite3_bind_blob64(stmt, pos, arg.ptr, arg.length, null);
		else
			return sqlite3_bind_blob(stmt, pos, arg.ptr, cast(int)arg.length, null);
	}

	int bindArg(T)(int pos, T arg) if (is(Unqual!T == typeof(null)))
	{
		return sqlite3_bind_null(stmt, pos);
	}

	T getArg(T)(int pos)
	{
		import core.stdc.string;

		int typ = sqlite3_column_type(stmt, pos);
		static if(isIntegral!T) {
			enforce!SQLiteException(typ == SQLITE_INTEGER,
					"Column is not an integer");
			static if(T.sizeof > 4)
				return cast(T)sqlite3_column_int64(stmt, pos);
			else
				return cast(T)sqlite3_column_int(stmt, pos);
		} else static if(isSomeString!T) {
			if (typ == SQLITE_NULL)
				return T.init;
			enforce!SQLiteException(typ == SQLITE3_TEXT,
					"Column is not a string");
			return cast(T)fromStringz(sqlite3_column_text(stmt, pos)).dup;
		} else static if(isFloatingPoint!T) {
			enforce!SQLiteException(typ == SQLITE_FLOAT,
					"Column is not a real");
			return sqlite3_column_double(stmt, pos);
		} else {
			if (typ == SQLITE_NULL)
				return T.init;
			enforce!SQLiteException(typ == SQLITE_BLOB,
					"Column is not a blob");
			auto ptr = sqlite3_column_blob(stmt, pos);
			int size = sqlite3_column_bytes(stmt, pos);
			static if(isStaticArray!T) {
				T arr = void;
				memcpy(arr.ptr, ptr, size);
				return arr;
			} else
				return cast(T)ptr[0..size].dup;
		}
	}

	static auto make(State state, string prefix, string suffix, T)(sqlite3* db, T s)
	if(isAggregateType!T) {
		mixin getSQLFields!(prefix, suffix, T);
		// Skips "rowid" field
		static if(I >= 0) {
			return Query(db, SQLBuilder(sql!sqlFields, state),
				s.tupleof[0..I], s.tupleof[I+1..$]);
		} else {
			return Query(db, SQLBuilder(sql!sqlFields, state), s.tupleof);
		}
	}

	void checkError(string prefix, int rc, string file = __FILE__, int line = __LINE__)
	{
		if(rc < 0)
			rc = sqlite3_errcode(db);
		if(rc != SQLITE_OK && rc != SQLITE_ROW && rc != SQLITE_DONE)
			throw new SQLiteException(prefix ~ " (" ~ rc.to!string ~ "): " ~
				sqlite3_errmsg(db).toStr, file, line);
	}

public:
	/// Bind these args in order to '?' marks in statement
	void set(ARGS...)(ARGS args)
	{
		static foreach(i, a; args)
			checkError("Bind failed: ", bindArg(i + 1, a));
	}

	// Find column by name
	int findColumn(string name)
	{
		import core.stdc.string : strcmp;

		auto ptr = name.toz;
		int count = sqlite3_column_count(stmt);
		for(int i = 0; i < count; i++) {
			if(strcmp(sqlite3_column_name(stmt, i), ptr) == 0)
				return i;
		}
		return -1;
	}

	/// Get current row (and column) as a basic type
	T get(T, int COL = 0)() if(!(isAggregateType!T))
	{
		if(lastCode == -1)
			step();
		return getArg!T(COL);
	}

	/// Map current row to the fields of the given T
	T get(T, int _ = 0)() if(isAggregateType!T)
	{
		if(lastCode == -1)
			step();
		T t;
		int i = void;
		static foreach(N; FieldNameTuple!T) {
			i = findColumn(ColumnName!(T, N));
			if(i >= 0)
				__traits(getMember, t, N) = getArg!(typeof(__traits(getMember, t, N)))(i);
		}
		return t;
	}

	/// Get current row as a tuple
	Tuple!T get(T...)()
	{
		Tuple!T t = void;
		foreach(I, Ti; T)
			t[I] = get!(Ti, I)();
		return t;
	}

	/// Step the SQL statement; move to next row of the result set. Return `false` if there are no more rows
	bool step()
	in(stmt) {
		lastCode = sqlite3_step(stmt);
		checkError("Step failed", lastCode);
		return lastCode == SQLITE_ROW;
	}

	/// Reset the statement, to step through the resulting rows again.
	void reset() in(stmt) { sqlite3_reset(stmt); }

	void spin() { while (step()) {} }

	static auto insert(OR OPTION = OR.None, T)(sqlite3* db, T s)
	if(isAggregateType!T) in(db !is null) {
		import std.array : replicate;

		enum qms = ",?".replicate(ColumnCount!T);
		return make!(State.insert, OPTION ~ "INTO " ~
			quote(SQLName!T) ~ "(", ") VALUES(" ~
				(qms.length ? qms[1..$] : qms) ~ ")")(db, s);
	}

	///
	unittest {
		User u = { name: "jonas", age: 13 };
		Message m = { contents : "some text" };
		assert(Query.insert(u) == "INSERT INTO 'User'('name','age') VALUES(?,?)");
		assert(Query.insert(m) == "INSERT INTO 'msg'('contents') VALUES(?)");
	}

	///
	auto update(OR OPTION = OR.None, T)(sqlite3* db, T s) if(isAggregateType!T) {
		return make!(State.set, "UPDATE " ~ OPTION ~ SQLName!T ~
			" SET ", "=?")(db, s);
	}

	///
	unittest {
		User user = { name: "Jonas", age: 34 };
		assert(Query.update(user) == "UPDATE User SET 'name'=?,'age'=?");
	}
}

///
unittest {
	mixin TEST!"query";

	auto q = Query(db, "create table TEST(a INT, b INT)");
	assert(!q.step());

	q = Query(db, "insert into TEST values(?, ?)");
	q.set(1, 2);
	assert(!q.step());
	q = Query(db, "select b from TEST where a == ?", 1);
	assert(q.step());
	assert(q.get!int == 2);
	assert(!q.step());

	q = Query(db, "select a,b from TEST where b == ?", 2);
	// Try not stepping... assert(q.step());
	assert(q.get!(int, int) == tuple(1, 2));

	struct Test { int a, b; }

	auto test = q.get!Test;
	assert(test.a == 1 && test.b == 2);

	assert(!q.step());

	q.reset();
	assert(q.step());
	assert(q.get!(int, int) == tuple(1, 2));

	// Test exception
	assertThrown!SQLiteException(q.get!string);
}

/// A sqlite3 database
class SQLite3
{

	/** Create a SQLite3 from a database file. If file does not exist, the
	  * database will be initialized as new
	 */
	this(string dbFile, int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, int busyTimeout = 500)
	{
		int rc = sqlite3_open_v2(dbFile.toz, &db, flags, null);
		if (!rc)
			sqlite3_busy_timeout(db, busyTimeout);
		if(rc != SQLITE_OK) {
			auto errmsg = fromStringz(sqlite3_errmsg(db)).idup;
			sqlite3_close(db);
			db = null;
			throw new SQLiteException("Could not open database:" ~ errmsg);
		}
	}

	/// Execute multiple statements
	int execSQL(string sql, out string errmsg)
	{
		char* err_msg = void;
		int rc = sqlite3_exec(db, sql.toz, null, null, &err_msg);
		errmsg = fromStringz(err_msg).idup;
		return rc;
	}

	/// Execute an sql statement directly, binding the args to it
	bool exec(ARGS...)(string sql, ARGS args)
	{
		auto q = Query(db, sql, args);
		q.step();
		return q.lastCode == SQLITE_DONE || q.lastCode == SQLITE_ROW;
	}

	///
	unittest {
		mixin TEST!"exec";
		assert(db.exec("CREATE TABLE Test(name STRING)"));
		assert(db.exec("INSERT INTO Test VALUES (?)", "hey"));
	}

	/// Return 'true' if database contains the given table
	bool hasTable(string table)
	{
		return query("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
			table).step();
	}

	///
	unittest {
		mixin TEST!"hastable";
		assert(!db.hasTable("MyTable"));
		db.exec("CREATE TABLE MyTable(id INT)");
		assert(db.hasTable("MyTable"));
	}

	/// Return the 'rowid' produced by the last insert statement
	@property long lastRowid() { return sqlite3_last_insert_rowid(db); }

	///
	unittest {
		mixin TEST!"lastrowid";
		assert(db.exec("CREATE TABLE MyTable(name STRING)"));
		assert(db.exec("INSERT INTO MyTable VALUES (?)", "hey"));
		assert(db.lastRowid == 1);
		assert(db.exec("INSERT INTO MyTable VALUES (?)", "ho"));
		assert(db.lastRowid == 2);
		// Only insert updates the last rowid
		assert(db.exec("UPDATE MyTable SET name=? WHERE rowid=?", "woo", 1));
		assert(db.lastRowid == 2);
	}

	/// Create query from string and args to bind
	Query query(ARGS...)(string sql, ARGS args)
	{
		return Query(db, sql, args);
	}

	bool commit() { return exec("commit"); }
	bool begin() { return exec("begin"); }
	bool rollback() { return exec("rollback"); }

	unittest {
		mixin TEST!"transaction";
		db.begin();
		assert(db.exec("CREATE TABLE MyTable(name STRING)"));
		assert(db.exec("INSERT INTO MyTable VALUES (?)", "hey"));
		db.rollback();
		assert(!db.hasTable("MyTable"));
		db.begin();
		assert(db.exec("CREATE TABLE MyTable(name STRING)"));
		assert(db.exec("INSERT INTO MyTable VALUES (?)", "hey"));
		db.commit();
		assert(db.hasTable("MyTable"));
	}

	protected sqlite3 *db;
	alias db this;
}