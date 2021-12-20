module sqlite3_d.sqlbuilder;

import sqlite3_d.utils;

version(unittest) package {
	struct User {
		string name;
		int age;
	}

	@as("msg") struct Message {
		@as("rowid") int id;
		string contents;
	}
}
import
	std.meta,
	std.range,
	std.traits;
import std.typecons : tuple;
import std.string : join, count;

/// Get the sqlname of `STRUCT`
template SQLName(alias STRUCT, string defaultName = STRUCT.stringof) {
	static if(hasUDA!(STRUCT, as))
		enum SQLName = getUDAs!(STRUCT, as)[0].name;
	else
		enum SQLName = defaultName;
};

///
unittest {
	assert(SQLName!User == "User");
	assert(SQLName!Message == "msg");
}

/// Generate a column name given a FIELD in STRUCT.
template ColumnName(STRUCT, string FIELD) if(isAggregateType!STRUCT) {
	enum ColumnName = SQLName!(__traits(getMember, STRUCT, FIELD), FIELD);
}

/// Return the qualifed column name of the given struct field
enum ColumnName(alias FIELD) =
	quote(SQLName!(__traits(parent, FIELD))) ~ "." ~ quote(SQLName!FIELD);

///
unittest {
	@as("msg") struct Message {
		@as("txt") string contents;
	}

	assert(ColumnName!(User, "age") == "age");
	assert(ColumnName!(Message.contents) == "'msg'.'txt'");
	assert(ColumnName!(User.age) == "'User'.'age'");
}

template ColumnNames(STRUCT) {
	enum colName(string NAME) = ColumnName!(STRUCT, NAME);
	enum ColumnNames = staticMap!(colName, FieldNameTuple!STRUCT);
}

/// get column count except "rowid" field
template ColumnCount(STRUCT) {
	enum
		colNames = ColumnNames!STRUCT,
		indexOfRowid = staticIndexOf!("rowid", colNames);
	static if(indexOfRowid >= 0)
		enum ColumnCount = colNames.length - 1;
	else
		enum ColumnCount = colNames.length;
}

template sqlType(T) if(isSomeString!T) { enum sqlType = "TEXT"; }
template sqlType(T) if(isFloatingPoint!T) { enum sqlType = "REAL"; }
template sqlType(T) if(isIntegral!T || is(T == bool)) { enum sqlType = "INT"; }
template sqlType(T) if(!isSomeString!T && !isScalarType!T) { enum sqlType = "BLOB"; }

bool checkField(string F, TABLES...)() {
	foreach (TABLE; TABLES) {
		enum tblName = SQLName!TABLE;
		foreach (N; FieldNameTuple!TABLE) {
			enum colName = ColumnName!(TABLE, N);
			if (colName == F || tblName ~ "." ~ colName == F ||
				colName.quote == F ||
				tblName.quote ~ "." ~ colName.quote == F)
				return true;
		}
	}
	return false;
}

template checkFields(string[] FIELDS, TABLES...)
{
	enum check(string F) = checkField!(F, TABLES);
	enum checkFields = allSatisfy!(check, aliasSeqOf!FIELDS);
}

enum State {
	None = "",
	Create = "CREATE TABLE ",
	CreateIfNE = "CREATE TABLE IF NOT EXISTS ",
	Delete = "DELETE FROM ",
	From = " FROM ",
	Insert = "INSERT ",
	Select = "SELECT ",
	Set = " SET ",
	Update = "UPDATE ",
	Where = " WHERE ",
}

enum OR {
	None = "",
	Abort = "OR ABORT ",
	Fail = "OR FAIL ",
	Ignore = "OR IGNORE ",
	Replace = "OR REPLACE ",
	Rollback = "OR ROLLBACK "
}

/** An instance of a query building process */
struct SQLBuilder(State STATE = State.None, ARGS...)
{
	static if(STATE == State.Select)
		alias Selects = Alias!(ARGS[0]),
			Args = AliasSeq!();
	else
		alias ARGS Args;

	Args args;
	string sql;
	alias sql this;
private:

	alias SB(T...) = SQLBuilder!T;

	static auto make(State STATE, string prefix, string suffix, STRUCT, Args...)(STRUCT s)
	if(isAggregateType!STRUCT) {
		enum
			colNames = ColumnNames!STRUCT,
			I = staticIndexOf!("rowid", colNames),
			sql(alias s) = prefix ~ s.quoteJoin(suffix == "=?" ? "=?," : ",")
				~ suffix;
		// Skips "rowid" field
		static if(I >= 0) {
			enum sqlFields = [CutOut!(I, colNames)];
			return SQLBuilder!(STATE, CutOut!(I, Fields!STRUCT))(sql!sqlFields,
					s.tupleof[0..I], s.tupleof[I+1..$]);
		} else {
			enum sqlFields = [colNames];
			return SQLBuilder!(STATE, Fields!STRUCT)(sql!sqlFields, s.tupleof);
		}
	}

	template VerifyParams(alias what, ARGS...) {
		static assert(what.count('?') == ARGS.length, "Incorrect number parameters");
	}
public:

	this(string sql, Args args) {
		static if(startsWithWhite!STATE)
			this.sql = sql;
		else
			this.sql = STATE ~ sql;
		static if(STATE != State.Select)
			this.args = args;
	}

	static auto create(STRUCT)() if(isAggregateType!STRUCT)
	{
		enum SATTRS = getAttr!STRUCT;
		string s;
		static foreach(A; SATTRS)
			static if(isSomeString!(typeof(A)))
				static if(A.length) {
					static if(startsWithWhite!A)
						s ~= A;
					else
						s ~= " " ~ A;
				}
		alias FIELDS = Fields!STRUCT;
		string[] fields, keys, pkeys;

		foreach(I, N; FieldNameTuple!STRUCT) {
			enum ATTRS = __traits(getAttributes, __traits(getMember, STRUCT, N)),
				colName = ColumnName!(STRUCT, N);

			static if(colName != "rowid") {
				string field = quote(colName) ~ " ",
					   type = sqlType!(FIELDS[I]),
					   constraints;
			}
			static foreach(A; ATTRS)
				static if(is(typeof(A) == sqlkey)) {
					static if(A.key.length)
						keys ~= "FOREIGN KEY(" ~ colName ~ ") REFERENCES " ~ A.key;
					else
						pkeys ~= colName;
				} else static if(colName != "rowid" && is(typeof(A) == sqltype))
					type = A.type;
				else static if(isSomeString!(typeof(A))) {
					static if(A.length) {
						static if(startsWithWhite!A)
							constraints ~= A;
						else
							constraints ~= " " ~ A;
					}
				}
			static if(colName != "rowid") {
				import std.conv : to;

				field ~= type ~ constraints;
				enum MEMBER = __traits(getMember, STRUCT.init, N);
				if(MEMBER != FIELDS[I].init)
					field ~= " default " ~ quote(MEMBER.to!string);
				fields ~= field;
			}
		}
		if(pkeys)
			keys ~= "PRIMARY KEY(" ~ pkeys.join(',') ~ ")";

		return SB!(State.CreateIfNE)(quote(SQLName!STRUCT) ~
				"(" ~ join(fields ~ keys, ',') ~ ")" ~ s);
	}

	///
	unittest {
		assert(SQLBuilder.create!User == "CREATE TABLE IF NOT EXISTS 'User'('name' TEXT,'age' INT)");
		assert(!__traits(compiles, SQLBuilder().create!int));
	}

	static auto insert(OR OPTION = OR.None, STRUCT)(STRUCT s) if(isAggregateType!STRUCT)
	{
		import std.array : replicate;

		enum qms = ",?".replicate(ColumnCount!STRUCT);
		return make!(State.Insert, OPTION ~ "INTO " ~
			quote(SQLName!STRUCT) ~ "(", ") VALUES(" ~
				(qms.length ? qms[1..$] : qms) ~ ")")(s);
	}

	///
	unittest {
		User u = { name: "jonas", age: 13 };
		Message m = { contents : "some text" };
		assert(SQLBuilder.insert(u) == "INSERT INTO 'User'('name','age') VALUES(?,?)");
		assert(SQLBuilder.insert(m) == "INSERT INTO 'msg'('contents') VALUES(?)");
	}

	///
	static auto select(STRING...)() if (STRING.length)
	{
		return SB!(State.Select, STRING)([STRING].join(','));
	}
	///
	unittest {
		assert(SQLBuilder.select!("only_one") == "SELECT only_one");
		assert(SQLBuilder.select!("hey", "you") == "SELECT hey,you");
	}

	///
	static auto selectAllFrom(STRUCTS...)()
	{
		string[] fields, tables;
		static foreach(I, Ti; STRUCTS) {
			static foreach(N; FieldNameTuple!Ti)
				fields ~= quote(SQLName!Ti) ~ "." ~ quote(ColumnName!(Ti, N));

			tables ~= SQLName!Ti;
		}
		auto sql = "SELECT " ~ fields.join(',') ~ " FROM " ~ tables.quoteJoin(",");
		return SB!(State.From)(sql);
	}
	///
	unittest {
		assert(SQLBuilder.selectAllFrom!(Message, User) ==
			"SELECT 'msg'.'rowid','msg'.'contents','User'.'name','User'.'age' FROM 'msg','User'");
	}

	///
	auto from(S)(S tables) if(STATE == State.Select && isSomeString!S)
	{
		sql ~= " FROM " ~ tables;

		return SB!(State.From, Args)(sql, args);
	}

	///
	auto from(Strings...)(Strings tables) if(STATE == State.Select && allString!Strings)
	{
		return from([tables].join(','));
	}

	///
	auto from(TABLES...)() if(STATE == State.Select && allAggregate!TABLES)
	{
		static assert(checkFields!([Selects], TABLES), "Not all selected fields match column names");
		string[] tables;
		foreach(T; TABLES)
			tables ~= SQLName!T;
		return from(tables.quoteJoin(","));
	}

	///
	auto set(string what, A...)(A a) if(STATE == State.Update)
	{
		mixin VerifyParams!(what, A);
		return SB!(State.Set)(sql ~ " SET " ~ what, a);
	}

	static {
		///
		auto update(OR OPTION = OR.None, S)(S table) if(isSomeString!S)
		{
			return SB!(State.Update)(OPTION ~ table);
		}

		///
		auto update(OR OPTION = OR.None, STRUCT)(STRUCT s) if(isAggregateType!STRUCT)
		{
			return make!(State.Set, "UPDATE " ~ OPTION ~ SQLName!STRUCT ~
				" SET ", "=?")(s);
		}
	}

	///
	alias update(OR OPTION = OR.None, STRUCT) = update(SQLName!STRUCT);

	///
	unittest {
		User user = { name: "Jonas", age: 34 };
		assert(SQLBuilder.update(user) == "UPDATE User SET 'name'=?,'age'=?");
		assert(SQLBuilder.update("User") == "UPDATE User");
		//assert(SQLBuilder.update!User == "UPDATE User");
	}

	///
	auto where(string what, A...)(A args) if(STATE == State.Set ||
		STATE == State.From || STATE == State.Delete)
	{
		mixin VerifyParams!(what, A);
		return SB!(State.Where, Args, A)(sql ~ " WHERE " ~ what, AliasSeq!(this.args, args));
	}

	///
	static auto del(TABLE)() if(isAggregateType!TABLE)
	{
		return del(SQLName!TABLE);
	}

	///
	static auto del(S)(S tablename) if(isSomeString!S)
	{
		return SB!(State.Delete)(tablename);
	}

	///
	alias del delete_;

	///
	unittest {
		SQLBuilder.del!User.where!"name=?"("greg");
	}

	auto opCall(S)(S expr) if(isSomeString!S) {
		sql ~= expr;
		return this;
	}
}

///
unittest
{
	// This will map to a "User" table in our database
	struct User {
		string name;
		int age;
	}
	alias Q = SQLBuilder!(),
		  C = ColumnName;

	assert(Q.create!User == "CREATE TABLE IF NOT EXISTS 'User'('name' TEXT,'age' INT)");

	auto qb0 = Q.select!"name".from!User.where!"age=?"(12);

	// The properties `sql` and `bind` can be used to access the generated sql and the
	// bound parameters
	assert(qb0.sql == "SELECT name FROM 'User' WHERE age=?");
	assert(qb0.args == AliasSeq!12);

	/// We can decorate structs and fields to give them different names in the database.
	@as("msg") struct Message {
		@as("rowid") int id;
		string contents;
	}

	// Note that virtual "rowid" field is handled differently -- it will not be created
	// by create(), and not inserted into by insert()

	assert(Q.create!Message == "CREATE TABLE IF NOT EXISTS 'msg'('contents' TEXT)");

	Message m = { id : -1 /* Ignored */, contents : "Some message" };
	auto qb = Q.insert(m);
	assert(qb.sql == "INSERT INTO 'msg'('contents') VALUES(?)");
	assert(qb.args == AliasSeq!"Some message");
}

unittest
{
	import std.algorithm.iteration : uniq;
	import std.algorithm.searching : count;
	alias Q = SQLBuilder!(),
		  C = ColumnName;

	// Make sure all these generate the same sql statement
	auto sql = [
		Q.select!("'msg'.'rowid'", "'msg'.'contents'").from("'msg'").where!"'msg'.'rowid'=?"(1).sql,
		Q.select!("'msg'.'rowid'", "'msg'.'contents'").from!Message.where!(C!(Message.id) ~ "=?")(1).sql,
		Q.select!(C!(Message.id), C!(Message.contents)).from!Message.where!"'msg'.'rowid'=?"(1).sql,
		Q.selectAllFrom!Message.where!"'msg'.'rowid'=?"(1).sql
	];
	assert(count(uniq(sql)) == 1);
}