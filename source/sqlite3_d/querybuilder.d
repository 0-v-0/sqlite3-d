module sqlite3_d.querybuilder;

import sqlite3_d.utils;

version(unittest) package {
	struct User {
		string name;
		int age;
	}

	@sqlname("msg") struct Message {
		@sqlname("rowid") int id;
		string contents;
	}
}

import std.traits;
import std.typecons : tuple, Tuple;
import std.string : join, count;

/// Get the tablename of `STRUCT`
template TableName(alias STRUCT) {
	enum ATTRS = getAttr!STRUCT;
	static if(ATTRS.length > 0 && is(typeof(ATTRS[0]) == sqlname))
		enum TableName = ATTRS[0].name;
	else
		enum TableName = STRUCT.stringof;
};
///
unittest {
	assert(TableName!User == "User");
	assert(TableName!Message == "msg");
}

/// Generate a column name given a FIELD in STRUCT.
template ColumnName(STRUCT, string FIELD) if(isAggregateType!STRUCT) {
	enum ATTRS = __traits(getAttributes, __traits(getMember, STRUCT, FIELD));
	static if(ATTRS.length > 0 && is(typeof(ATTRS[0]) == sqlname))
		enum ColumnName = ATTRS[0].name;
	else
		enum ColumnName = FIELD;
}

/// Return the qualifed column name of the given struct field
template ColumnName(alias FIELDNAME)
{
	enum ATTRS = getAttr!FIELDNAME;
	static if(ATTRS.length > 0 && is(typeof(ATTRS[0]) == sqlname))
		enum CN = ATTRS[0].name;
	else
		enum CN = FIELDNAME.stringof;

	enum ColumnName = quote(TableName!(__traits(parent, FIELDNAME))) ~ "." ~ quote(CN);
}
///
unittest {
	@sqlname("msg") struct Message { @sqlname("txt") string contents; }
	assert(ColumnName!(User, "age") == "age");
	assert(tuple(ColumnName!(Message.contents), ColumnName!(User.age)) == tuple("'msg'.'txt'", "'User'.'age'"));
}

enum State {
	Select, Set, Empty, SetWhere, From, SelectWhere, Update, Create, Insert, Delete
};

enum OR {
	None = "",
	Rollback = "OR ROLLBACK ",
	Abort = "OR ABORT ",
	Replace = "OR REPLACE ",
	Fail = "OR FAIL ",
	Ignore = ""
}

/** An instance of a query building process */
struct QueryBuilder(State STATE = State.Empty, BINDS = Tuple!(), string[] SELECTS = [])
{
	BINDS args;
	string sql;
	alias sql this;

	@property BINDS binds() { return args; }

private:
	static bool checkField(string F, TABLES...)() pure nothrow {
		foreach(TABLE; TABLES) {
			enum tableName = TableName!TABLE;
			foreach(N; FieldNameTuple!TABLE) {
				enum colName = ColumnName!(TABLE,N);
				if(colName == F || tableName ~ "." ~ colName == F
				 || quote(colName) == F || quote(tableName) ~ "." ~ quote(colName) == F)
					return true;
			}
		}
		return false;
	}

	static bool checkFields(string[] FIELDS, TABLES...)()
	{
		static if(FIELDS.length > 1)
			return checkField!(FIELDS[0], TABLES) && checkFields!(FIELDS[1..$], TABLES);
		else
			return checkField!(FIELDS[0], TABLES);
	}

	static auto make(State STATE = State.Empty, string[] SELECTS = [], BINDS)(string sql, BINDS binds)
	{
		return QueryBuilder!(STATE, BINDS, SELECTS)(sql, binds);
	}

	template sqlType(T) if(isSomeString!T) { enum sqlType = "TEXT"; }
	template sqlType(T) if(isFloatingPoint!T) { enum sqlType = "REAL"; }
	template sqlType(T) if(isIntegral!T || is(T == bool)) { enum sqlType = "INT"; }
	template sqlType(T) if(!isSomeString!T && !isScalarType!T) { enum sqlType = "BLOB"; }
	mixin template VerifyParams(string what, ARGS...)
	{
		static assert(what.count("?") == A.length, "Incorrect number parameters");
	}

public:

	this(string sql, BINDS args)
	{
		this.sql = sql;
		this.args = args;
	}

	static auto create(STRUCT)() if(isAggregateType!STRUCT)
	{
		enum SATTRS = getAttr!STRUCT;
		string s;
		static foreach(A; SATTRS)
			static if(is(typeof(A) == string) && A.length > 0) {
				import std.uni : isWhite;
				static if(isWhite(A[0]))
					s ~= A;
				else
					s ~= " " ~ A;
			}
		alias FIELDS = Fields!STRUCT;
		string[] fields, keys, pkeys;

		foreach(I, N; FieldNameTuple!STRUCT) {
			alias colName = ColumnName!(STRUCT, N);
			enum ATTRS = __traits(getAttributes, __traits(getMember, STRUCT, N));
			static if(colName != "rowid") {
				string field = quote(colName) ~ " ",
					   type = sqlType!(FIELDS[I]),
					   constraints;
			}
			foreach(A; ATTRS)
				static if(is(typeof(A) == sqlkey)) {
					static if(A.key == "")
						pkeys ~= colName;
					else
						keys ~= "FOREIGN KEY(" ~ colName ~ ") REFERENCES " ~ A.key;
				} else static if(colName != "rowid" && is(typeof(A) == sqltype)) {
					type = A.type;
				} else static if(is(typeof(A) == string) && A.length > 0) {
					import std.uni : isWhite;
					static if(isWhite(A[0]))
						constraints ~= A;
					else
						constraints ~= " " ~ A;
				}
			static if(colName != "rowid") {
				import std.conv : to;

				field ~= type ~ constraints;
				enum MEMBER = __traits(getMember, STRUCT.init, N);
				if(MEMBER != FIELDS[I].init)
					field ~= " default " ~ quote(to!string(MEMBER));
				fields ~= field;
			}
		}
		if(pkeys)
			keys ~= "PRIMARY KEY(" ~ pkeys.join(",") ~ ")";

		return make!(State.Create)("CREATE TABLE IF NOT EXISTS " ~ quote(TableName!STRUCT) ~
				"(" ~ join(fields ~ keys, ",") ~ ")" ~ s, tuple());
	}

	///
	unittest {
		assert(QueryBuilder.create!User == "CREATE TABLE IF NOT EXISTS 'User'('name' TEXT,'age' INT)");
		assert(!__traits(compiles, QueryBuilder().create!int));
	}

	// Get all field names in `s` to `fields`, and return the contents
	// of all fields as a tuple. Skips "rowid" fields.
	static auto getFields(STRUCT, int n = 0)(STRUCT s, ref string []fields)
	{
		enum L = Fields!STRUCT.length;
		static if(n == L)
			return tuple();
		else {
			enum NAME = (FieldNameTuple!STRUCT)[n],
				 CN = ColumnName!(STRUCT, NAME);
			static if(CN == "rowid")
				return tuple(getFields!(STRUCT, n+1)(s, fields).expand);
			else {
				fields ~= CN;
				return tuple(s.tupleof[n], getFields!(STRUCT, n+1)(s, fields).expand);
			}
		}
	}

	static auto insert(OR OPTION = OR.None, STRUCT)(STRUCT s) if(isAggregateType!STRUCT)
	{
		import std.algorithm.iteration : map;
		import std.array : replicate;

		string[] fields;
		auto t = getFields(s, fields);
		auto qms = ",?".replicate(fields.length);
		if(qms.length) qms = qms[1..$];
		return make!(State.Insert)("INSERT " ~ OPTION ~ "INTO " ~ quote(TableName!STRUCT) ~ "(" ~ quote(fields).join(",") ~ ") VALUES(" ~ qms ~ ")", t);
	}

	///
	unittest {
		User u = { name : "jonas", age : 13 };
		Message m = { contents : "some text" };
		assert(QueryBuilder.insert(u) == "INSERT INTO 'User'('name','age') VALUES(?,?)");
		assert(QueryBuilder.insert(m) == "INSERT INTO 'msg'('contents') VALUES(?)");
	}

	///
	static auto select(STRING...)()
	{
		const arr = [STRING];
		auto sql = "SELECT " ~ arr.join(",");
		return make!(State.Select, arr)(sql, tuple());
	}
	///
	unittest {
		assert(QueryBuilder.select!("only_one") == "SELECT only_one");
		assert(QueryBuilder.select!("hey", "you") == "SELECT hey,you");
	}

	///
	static auto selectAllFrom(STRUCTS...)()
	{
		string[] fields, tables;
		foreach(I, Ti; STRUCTS) {
			enum TABLE = TableName!Ti;

			alias NAMES = FieldNameTuple!Ti;
			foreach(N; NAMES)
				fields ~= quote(TABLE) ~ "." ~ quote(ColumnName!(Ti, N));

			tables ~= TABLE;
		}
		auto sql = "SELECT " ~ fields.join(",") ~ " FROM " ~ quote(tables).join(",");
		return make!(State.From, [])(sql, tuple());
	}
	///
	unittest {
		assert(QueryBuilder.selectAllFrom!(Message, User) == "SELECT 'msg'.'rowid','msg'.'contents','User'.'name','User'.'age' FROM 'msg','User'");
	}

	///
	auto from(TABLES...)() if(STATE == State.Select && allString!TABLES)
	{
		sql ~= " FROM " ~ [TABLES].join(",");

		return make!(State.From, SELECTS)(sql, args);
	}

	///
	auto from(TABLES...)() if(STATE == State.Select && allAggregate!TABLES)
	{
		static assert(checkFields!(SELECTS, TABLES), "Not all selected fields match column names");
		string[] tables;
		foreach(T; TABLES)
			tables ~= TableName!T;
		sql ~= " FROM " ~ quote(tables).join(",");
		return make!(State.From, SELECTS)(sql, args);
	}

	///
	auto set(string what, A...)(A a) if(STATE == State.Update)
	{
		mixin VerifyParams!(what, A);
		return make!(State.Set)(sql ~ " SET " ~ what, tuple(a));
	}

	static {
		///
		auto update(string table)()
		{
			return make!(State.Update)("UPDATE " ~ table, tuple());
		}

		///
		auto update(STRUCT)()
		{
			return make!(State.Update)("UPDATE " ~ TableName!STRUCT, tuple());
		}

		///
		auto update(STRUCT)(STRUCT s)
		{
			string[] fields;
			auto t = getFields(s, fields);
			return make!(State.Set)("UPDATE " ~ TableName!STRUCT ~ " SET " ~ join(fields, "=?,") ~ "=?", t);
		}
	}

	///
	unittest {
		User user = { name : "Jonas", age : 34 };
		assert(QueryBuilder.update(user) == "UPDATE User SET name=?,age=?");
	}

	///
	auto where(string what, A...)(A args) if(STATE == State.Set)
	{
		mixin VerifyParams!(what, A);
		return make!(State.SetWhere, SELECTS)(sql ~ " WHERE " ~ what, tuple(this.args.expand, args));
	}

	///
	auto where(string what, A...)(A args) if(STATE == State.From)
	{
		mixin VerifyParams!(what, A);
		return make!(State.SelectWhere, SELECTS)(sql ~ " WHERE " ~ what, tuple(this.args.expand, args));
	}

	///
	auto where(string what, A...)(A args) if(STATE == State.Delete)
	{
		mixin VerifyParams!(what, A);
		return make!(State.SelectWhere, SELECTS)(sql ~ " WHERE " ~ what, tuple(this.args.expand, args));
	}

	///
	static auto delete_(TABLE)() if(isAggregateType!TABLE)
	{
		return make!(State.Delete)("DELETE FROM " ~ TableName!TABLE, tuple());
	}

	///
	static auto delete_(string tablename)()
	{
		return make!(State.Delete)("DELETE FROM " ~ tablename);
	}
	///
	unittest {
		QueryBuilder.delete_!User.where!"name=?"("greg");
	}

	alias del = delete_;
}

///
unittest
{
	// This will map to a "User" table in our database
	struct User {
		string name;
		int age;
	}
	alias Q = QueryBuilder!(),
		  C = ColumnName;

	assert(Q.create!User == "CREATE TABLE IF NOT EXISTS 'User'('name' TEXT,'age' INT)");

	auto qb0 = Q.select!"name".from!User.where!"age=?"(12);

	// The properties `sql` and `bind` can be used to access the generated sql and the
	// bound parameters
	assert(qb0.sql == "SELECT name FROM 'User' WHERE age=?");
	assert(qb0.binds == tuple(12));

	/// We can decorate structs and fields to give them different names in the database.
	@sqlname("msg") struct Message {
		@sqlname("rowid") int id;
		string contents;
	}

	// Note that virtual "rowid" field is handled differently -- it will not be created
	// by create(), and not inserted into by insert()

	assert(Q.create!Message == "CREATE TABLE IF NOT EXISTS 'msg'('contents' TEXT)");

	Message m = { id : -1 /* Ignored */, contents : "Some message" };
	auto qb = Q.insert(m);
	assert(qb.sql == "INSERT INTO 'msg'('contents') VALUES(?)");
	assert(qb.binds == tuple("Some message"));
}

unittest
{
	import std.algorithm.iteration : uniq;
	import std.algorithm.searching : count;
	alias Q = QueryBuilder!(),
		  C = ColumnName;

	// Make sure all these generate the same sql statement
	auto sql = [
		Q.select!("'msg'.'rowid'", "'msg'.'contents'").from!("'msg'").where!"'msg'.'rowid'=?"(1).sql,
		Q.select!("'msg'.'rowid'", "'msg'.'contents'").from!Message.where!(C!(Message.id) ~ "=?")(1).sql,
		Q.select!(C!(Message.id), C!(Message.contents)).from!Message.where!"'msg'.'rowid'=?"(1).sql,
		Q.selectAllFrom!Message.where!"'msg'.'rowid'=?"(1).sql
	];
	assert(count(uniq(sql)) == 1);
}