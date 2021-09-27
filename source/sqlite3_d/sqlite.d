module sqlite3_d.sqlite;

import std.typecons : RefCounted, tuple, Tuple;
import std.traits;
import std.string;
import std.conv : to;
import etc.c.sqlite3;
import std.exception : enforce;
import sqlite3_d.utils;

pragma(lib, "sqlite3");

class db_exception : Exception {
	this(string msg, string file = __FILE__, size_t line = __LINE__) { super(msg, file, line); }
};

/// Setup code for tests
version(unittest) package mixin template TEST(string dbname)
{
	SQLite3 db = () {
		tryRemove(dbname ~ ".db");
		return new SQLite3(dbname ~ ".db");
	}();
}

/// A sqlite3 database
class SQLite3
{
	struct Statement
	{
		~this() { if(s) sqlite3_finalize(s); s = null; }
		sqlite3_stmt* s = null;
		alias s this;
	}

	/// Represents a sqlite3 statement
	struct Query
	{
		int lastCode;

		/// Construct a query from the string 'sql' into database 'db'
		this(ARGS...)(sqlite3* db, string sql, ARGS args)
		{
			lastCode = -1;
			sqlite3_stmt* s = null;
			int rc = sqlite3_prepare_v2(db, sql.toz, -1, &s, null);
			checkError("Prepare failed: ", rc);
			stmt.s = s;
			bind(args);
		}

		~this() { if(db) sqlite3_close(db); db = null; }

	private:
		sqlite3* db;
		RefCounted!Statement stmt;

		int bindArg(int pos, string arg)
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

		T getArg(T)(int pos)
		{
			int typ = sqlite3_column_type(stmt, pos);
			static if(isIntegral!T) {
				enforce!db_exception(typ == SQLITE_INTEGER,
						"Column is not an integer");
				static if(T.sizeof > 4)
					return cast(T)sqlite3_column_int64(stmt, pos);
				else
					return cast(T)sqlite3_column_int(stmt, pos);
			} else static if(isSomeString!T) {
				enforce!db_exception(typ == SQLITE3_TEXT,
						"Column is not a string");
				return fromStringz(sqlite3_column_text(stmt, pos)).idup;
			} else static if(isFloatingPoint!T) {
				enforce!db_exception(typ == SQLITE_FLOAT,
						"Column is not a real");
				return sqlite3_column_double(stmt, pos);
			} else {
				enforce!db_exception(typ == SQLITE_BLOB,
						"Column is not a blob");
				auto ptr = sqlite3_column_blob(stmt, pos);
				int size = sqlite3_column_bytes(stmt, pos);
				return cast(T)ptr[0..size].dup;
			}
		}

		void getArg(T)(int pos, ref T t)
		{
			t = getArg!T(pos);
		}

	public:
		/// Bind these args in order to '?' marks in statement
		void bind(ARGS...)(ARGS args)
		{
			foreach(i, a; args) {
				int rc = bindArg(i + 1, a);
				checkError("Bind failed: ", rc);
			}
		}

		// Find column by name
		int findColumn(string name)
		{
			import core.stdc.string : strcmp;

			auto zname = name.toz;
			for(int i=0; i<sqlite3_column_count(stmt); i++) {
				if(strcmp(sqlite3_column_name(stmt, i), zname) == 0)
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

		/// Map current row to the fields of the given STRUCT
		T get(T, int _ = 0)() if(isAggregateType!T)
		{
			if(lastCode == -1)
				step();
			T t;
			foreach(N; FieldNameTuple!T) {
				enum ATTRS = __traits(getAttributes, __traits(getMember, T, N));
				static if(ATTRS.length > 0 && is(typeof(ATTRS[0]) == sqlname))
					enum colName = ATTRS[0].name;
				else
					enum colName = N;
				getArg(findColumn(colName), __traits(getMember, t, N));
			}
			return t;
		}

		/// Get current row as a tuple
		Tuple!T get(T...)()
		{
			Tuple!T t;
			foreach(I, Ti; T)
				t[I] = get!(Ti, I)();
			return t;
		}

		/// Step the SQL statement; move to next row of the result set. Return `false` if there are no more rows
		bool step()
		{
			lastCode = sqlite3_step(stmt);
			checkError("Step failed", lastCode);
			return lastCode == SQLITE_ROW;
		}

		/// Reset the statement, to step through the resulting rows again.
		void reset() { sqlite3_reset(stmt); }

		void spin() { while (step()) {} }

		private void checkError(string prefix, int rc, string file = __FILE__, int line = __LINE__)
		{
			if(rc < 0)
				rc = sqlite3_errcode(db);
			if(rc != SQLITE_OK && rc != SQLITE_ROW && rc != SQLITE_DONE)
				throw new db_exception(prefix ~ " (" ~ rc.to!string ~ "): " ~
					sqlite3_errmsg(db).toStr, file, line);
		}
	}

	///
	unittest {
		mixin TEST!"query";

		auto q = Query(db, "create table TEST(a INT, b INT)");
		assert(!q.step());

		q = Query(db, "insert into TEST values(?, ?)");
		q.bind(1,2);
		assert(!q.step());
		q = Query(db, "select b from TEST where a == ?", 1);
		assert(q.step());
		assert(q.get!int == 2);
		assert(!q.step());

		q = Query(db, "select a,b from TEST where b == ?", 2);
		// Try not stepping... assert(q.step());
		assert(q.get!(int,int) == tuple(1,2));

		struct Test { int a, b; }

		auto test = q.get!Test;
		assert(test.a == 1 && test.b == 2);

		assert(!q.step());

		q.reset();
		assert(q.step());
		assert(q.get!(int, int) == tuple(1,2));

		// Test exception
		bool caught = false;
		try {
			q.get!string;
		} catch(db_exception e) {
			caught = true;
		}
		assert(caught);
	}

	/** Create a SQLite3 from a database file. If file does not exist, the
	  * database will be initialized as new
	 */
	this(string dbFile, int busyTimeout = 500)
	{
		int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
		int rc = sqlite3_open_v2(dbFile.toz, &db, flags, null);
		if (!rc)
			sqlite3_busy_timeout(db, busyTimeout);
		if(rc != SQLITE_OK) {
			auto errmsg = fromStringz(sqlite3_errmsg(db)).idup;
			sqlite3_close(db);
			db = null;
			throw new db_exception("Could not open database:" ~ errmsg);
		}
	}

	/// Execute multiple statements
	int execSQL(string sql, out string errmsg)
	{
		char* err_msg;
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
		mixin TEST!("exec");
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
		mixin TEST!("hastable");
		assert(!db.hasTable("MyTable"));
		db.exec("CREATE TABLE MyTable(id INT)");
		assert(db.hasTable("MyTable"));
	}

	/// Return the 'rowid' produced by the last insert statement
	@property long lastRowid() { return sqlite3_last_insert_rowid(db); }

	///
	unittest {
		mixin TEST!("lastrowid");
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

	/// Create query from QueryBuilder like class
	Query query(SOMEQUERY)(SOMEQUERY sq) if(hasMember!(SOMEQUERY, "sql") && hasMember!(SOMEQUERY, "binds"))
	{
		return Query(db, sq.sql, sq.binds.expand);
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