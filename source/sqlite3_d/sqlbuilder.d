module sqlite3_d.sqlbuilder;

import
	sqlite3_d.utils,
	std.exception,
	std.meta,
	std.range;
import std.string : join, count;

version(unittest) package {
	struct User {
		string name;
		int age;
	}

	@as("msg") struct Message {
		@as("rowid") int id;
		string contents;
	}

	alias Q = SQLBuilder;
}

/// Get the sqlname of `T`
template SQLName(alias T, string defaultName = T.stringof) {
	static if(hasUDA!(T, ignore))
		enum SQLName = "";
	else static if(hasUDA!(T, as))
		enum SQLName = getUDAs!(T, as)[0].name;
	else static if(hasUDA!(T, uncamel))
		enum SQLName = unCamelCase(defaultName);
	else
		enum SQLName = defaultName;
};

///
unittest {
	assert(SQLName!User == "User");
	assert(SQLName!Message == "msg");
}

/// Generate a column name given a FIELD in T.
template ColumnName(T, string FIELD) if(isAggregateType!T) {
	enum ColumnName = SQLName!(__traits(getMember, T, FIELD), FIELD);
}

/// Return the qualifed column name of the given struct field
enum ColumnName(alias FIELD) =
	quote(SQLName!(__traits(parent, FIELD))) ~ '.' ~ quote(SQLName!FIELD);

///
unittest {
	@as("msg") struct Message {
		@as("txt") string contents;
	}

	assert(ColumnName!(User, "age") == "age");
	assert(ColumnName!(Message.contents) == "'msg'.'txt'");
	assert(ColumnName!(User.age) == "'User'.'age'");
}

template ColumnNames(T) {
	enum colName(string NAME) = ColumnName!(T, NAME);
	enum ColumnNames = staticMap!(colName, FieldNameTuple!T);
}

/// get column count except "rowid" field
template ColumnCount(T) {
	enum
		colNames = ColumnNames!T,
		indexOfRowid = staticIndexOf!("rowid", colNames);
	static if(indexOfRowid >= 0)
		enum ColumnCount = colNames.length - 1;
	else
		enum ColumnCount = colNames.length;
}

template SQLTypeOf(T) {
	static if(isSomeString!T)
		enum SQLTypeOf = "TEXT";
	else static if(isFloatingPoint!T)
		enum SQLTypeOf = "REAL";
	else static if(isIntegral!T || is(T == bool))
		enum SQLTypeOf = "INT";
	else static if(!isSomeString!T && !isScalarType!T)
		enum SQLTypeOf = "BLOB";
	else static assert(0, "Unsupported SQLType '" ~ T.stringof ~ '.');
}

bool checkField(TABLES...)(string field) {
	foreach (TABLE; TABLES) {
		enum tblName = SQLName!TABLE;
		foreach (N; FieldNameTuple!TABLE) {
			enum colName = ColumnName!(TABLE, N);
			if (colName == field || tblName ~ '.' ~ colName == field ||
				colName.quote == field ||
				tblName.quote ~ '.' ~ colName.quote == field)
				return true;
		}
	}
	return false;
}

bool checkFields(TABLES...)(string[] fields) {
	foreach (field; fields)
		if(!checkField!TABLES(field))
			return false;
	return true;
}

enum State {
	none = "",
	create = "CREATE TABLE ",
	createIfNE = "CREATE TABLE IF NOT EXISTS ",
	del = "DELETE FROM ",
	from = " FROM ",
	groupBy = " GROUP BY ",
	having = " HAVING ",
	insert = "INSERT ",
	limit = " LIMIT ",
	offset = " OFFSET ",
	orderBy = " ORDER BY ",
	select = "SELECT ",
	set = " SET ",
	update = "UPDATE ",
	where = " WHERE "
}

enum OR {
	None = "",
	Abort = "OR ABORT ",
	Fail = "OR FAIL ",
	Ignore = "OR IGNORE ",
	Replace = "OR REPLACE ",
	Rollback = "OR ROLLBACK "
}

enum Clause(string name, prevStates...) =
	"SB " ~ name ~ "(S)(S expr) if(isSomeString!S)
		in(state is State." ~ [prevStates].join(" || state is State.") ~ ") {
		sql ~= (state = State." ~ name ~ ") ~ expr;
		return this;}";

/** An instance of a query building process */
struct SQLBuilder
{
	string sql;
	alias sql this;
	State state;
private:

	alias SQLBuilder SB;

	template make(string prefix, string suffix, T) if(isAggregateType!T) {
		mixin getSQLFields!(prefix, suffix, T);
		enum make = sql!sqlFields;
	}
public:

	this(string sql, State STATE = State.none) {
		this.sql = STATE.startsWithWhite ? sql : STATE ~ sql;
		state = STATE;
	}

	static SB create(T)() if(isAggregateType!T)
	{
		import std.conv : to;

		string s;
		static foreach(A; __traits(getAttributes, T))
			static if(isSomeString!(typeof(A)))
				static if(A.length) {
					static if(A.startsWithWhite)
						s ~= A;
					else
						s ~= ' ' ~ A;
				}
		alias FIELDS = Fields!T;
		string[] fields, keys, pkeys;

		static foreach(I, colName; ColumnNames!T)
		static if(colName.length) {{
			static if(colName != "rowid") {
				string field = quote(colName) ~ ' ',
					   type = SQLTypeOf!(FIELDS[I]),
					   constraints;
			}
			static foreach(A; __traits(getAttributes, T.tupleof[I]))
				static if(is(typeof(A) == sqlkey)) {
					static if(A.key.length)
						keys ~= "FOREIGN KEY(" ~ colName ~ ") REFERENCES " ~ A.key;
					else
						pkeys ~= colName;
				} else static if(colName != "rowid" && is(typeof(A) == sqltype))
					type = A.type;
				else static if(isSomeString!(typeof(A))) {
					static if(A.length) {
						static if(A.startsWithWhite)
							constraints ~= A;
						else
							constraints ~= ' ' ~ A;
					}
				}
			static if(colName != "rowid") {
				field ~= type ~ constraints;
				enum MEMBER = T.init.tupleof[I];
				if(MEMBER != FIELDS[I].init)
					field ~= " default " ~ quote(MEMBER.to!string);
				fields ~= field;
			}
		}}
		if(pkeys)
			keys ~= "PRIMARY KEY(" ~ pkeys.join(',') ~ ')';

		return SB(quote(SQLName!T) ~ '(' ~ join(fields ~ keys, ',') ~ ')'
				~ s, State.createIfNE);
	}

	///
	unittest {
		assert(SQLBuilder.create!User == "CREATE TABLE IF NOT EXISTS 'User'('name' TEXT,'age' INT)");
		assert(!__traits(compiles, SQLBuilder().create!int));
	}

	alias insert(T) = insert!(OR.None, T);

	static SB insert(OR or = OR.None, T)() if(isAggregateType!T)
	{
		import std.array : replicate;

		enum qms = ",?".replicate(ColumnCount!T);
		return SQLBuilder(make!(or ~ "INTO " ~
			quote(SQLName!T) ~ '(', ") VALUES(" ~
				(qms.length ? qms[1..$] : qms) ~ ')', T), State.insert);
	}

	///
	unittest {
		assert(SQLBuilder.insert!User == "INSERT INTO 'User'('name','age') VALUES(?,?)");
		assert(SQLBuilder.insert!Message == "INSERT INTO 'msg'('contents') VALUES(?)");
	}

	///
	static SB select(STRING...)() if (STRING.length)
	{
		return SB([STRING].join(','), State.select);
	}
	///
	unittest {
		assert(SQLBuilder.select!("only_one") == "SELECT only_one");
		assert(SQLBuilder.select!("hey", "you") == "SELECT hey,you");
	}

	///
	static SB selectAllFrom(STRUCTS...)()
	{
		string[] fields, tables;
		static foreach(I, S; STRUCTS) {{
			enum tblName = SQLName!S;
			static foreach(N; FieldNameTuple!S)
				fields ~= tblName.quote ~ '.' ~ ColumnName!(S, N).quote;

			tables ~= tblName;
		}}
		return SB("SELECT " ~ fields.join(',') ~ " FROM "
			~ tables.quoteJoin(","), State.from);
	}
	///
	unittest {
		assert(SQLBuilder.selectAllFrom!(Message, User) ==
			"SELECT 'msg'.'rowid','msg'.'contents','User'.'name','User'.'age' FROM 'msg','User'");
	}

	///
	mixin(Clause!("from", "select"));

	///
	SB from(Strings...)(Strings tables)
	if(Strings.length > 1 && allSatisfy!(isSomeString, Strings)) {
		return from([tables].join(','));
	}

	///
	SB from(TABLES...)()
	if(TABLES.length && allSatisfy!(isAggregateType, TABLES))
	in(checkFields!TABLES(sql[State.select.length..$].split(',')),
		"Not all selected fields match column names") {
		return from([staticMap!(SQLName, TABLES)].quoteJoin(","));
	}

	///
	mixin(Clause!("set", "update"));

	///
	static SB update(OR or = OR.None, S)(S table) if(isSomeString!S)
	{
		return SB(or ~ table, State.update);
	}

	///
	static SB update(T, OR or = OR.None)() if(isAggregateType!T) {
		return SQLBuilder(make!("UPDATE " ~ or ~ SQLName!T ~
			" SET ", "=?", T), State.set);
	}

	///
	unittest {
		assert(SQLBuilder.update("User") == "UPDATE User");
		assert(SQLBuilder.update!User == "UPDATE User SET 'name'=?,'age'=?");
	}

	///
	mixin(Clause!("where", "set", "from", "del"));

	///
	static SB del(TABLE)() if(isAggregateType!TABLE)
	{
		return del(SQLName!TABLE);
	}

	///
	static SB del(S)(S tablename) if(isSomeString!S)
	{
		return SB(tablename, State.del);
	}

	///
	alias del delete_;

	///
	unittest {
		SQLBuilder.del!User.where("name=?");
	}

	///
	mixin(Clause!("groupBy", "from", "where"));

	///
	mixin(Clause!("having", "from", "where", "groupBy"));

	///
	mixin(Clause!("orderBy", "from", "where", "groupBy", "having"));

	///
	mixin(Clause!("limit", "from", "where", "groupBy", "having", "orderBy"));

	///
	mixin(Clause!("offset", "limit"));

	SB opCall(S)(S expr) if(isSomeString!S) {
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

	assert(Q.create!User == "CREATE TABLE IF NOT EXISTS 'User'('name' TEXT,'age' INT)");

	auto qb0 = Q.select!"name".from!User.where("age=?");

	// The properties `sql` and `bind` can be used to access the generated sql and the
	// bound parameters
	assert(qb0.sql == "SELECT name FROM 'User' WHERE age=?");

	/// We can decorate structs and fields to give them different names in the database.
	@as("msg") struct Message {
		@as("rowid") int id;
		string contents;
	}

	// Note that virtual "rowid" field is handled differently -- it will not be created
	// by create(), and not inserted into by insert()

	assert(Q.create!Message == "CREATE TABLE IF NOT EXISTS 'msg'('contents' TEXT)");

	Message m = { id : -1 /* Ignored */, contents : "Some message" };
	auto qb = Q.insert!Message;
	assert(qb.sql == "INSERT INTO 'msg'('contents') VALUES(?)");
}

unittest
{
	import std.algorithm.iteration : uniq;
	import std.algorithm.searching : count;
	alias C = ColumnName;

	// Make sure all these generate the same sql statement
	auto sql = [
		Q.select!("'msg'.'rowid'", "'msg'.'contents'").from("'msg'").where("'msg'.'rowid'=?").sql,
		Q.select!("'msg'.'rowid'", "'msg'.'contents'").from!Message.where(C!(Message.id) ~ "=?").sql,
		Q.select!(C!(Message.id), C!(Message.contents)).from!Message.where("'msg'.'rowid'=?").sql,
		Q.selectAllFrom!Message.where("'msg'.'rowid'=?").sql
	];
	assert(count(uniq(sql)) == 1);
}