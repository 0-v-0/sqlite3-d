module sqlite3_d.database;

import
	etc.c.sqlite3,
	sqlite3_d;

/// Setup code for tests
version(unittest) template TEST(string dbname = "") {
	struct User {
		string name;
		int age;
	};

	struct Message {
		@as("rowid") int id;
		string content;
		int byUser;
	};

	mixin sqlite3_d.sqlite.TEST!(dbname, Database);
}

// Returned from select-type methods where the row type is known
struct QueryResult(T)
{
	Query query;
	alias query this;

	bool empty() {
		import etc.c.sqlite3;

		if (lastCode < 0)
			step();
		return lastCode != SQLITE_ROW;
	}
	void popFront() { step(); }
	T front() { return this.get!T; }
}

unittest {
	QueryResult!int qi;
	assert(qi.empty);
}

/// A Database with query building capabilities
class Database : SQLite3
{
	bool autoCreateTable = true;

	protected alias SQLBuilder SB;

	this(string name) { super(name); }

	bool create(T)()
	{
		auto q = super.query(SB.create!T);
		q.step();
		return q.lastCode == SQLITE_DONE;
	}

	auto selectAllWhere(T, string expr, ARGS...)(ARGS args) if(expr.length)
	{
		return QueryResult!T(super.query(SB.selectAllFrom!T.where(expr), args));
	}

	T selectOneWhere(T, string expr, ARGS...)(ARGS args) if(expr.length)
	{
		auto q = super.query(SB.selectAllFrom!T.where(expr), args);
		if(q.step())
			return q.get!T;
		throw new SQLEx("No match");
	}

	T selectOneWhere(T, string expr, T defValue = T.init, ARGS...)(ARGS args)
	if(expr.length) {
		auto q = super.query(SB.selectAllFrom!T.where(expr), args);
		return q.step() ? q.get!T : defValue;
	}

	T selectRow(T)(ulong row)
	{
		return selectOneWhere!(T, "rowid=?")(row);
	}

	unittest {
		mixin TEST;
		import std.array : array;
		import std.algorithm.iteration : fold;

		db.create!User;
		db.insert(User("jonas", 55));
		db.insert(User("oliver", 91));
		db.insert(User("emma", 12));
		db.insert(User("maria", 27));

		auto users = db.selectAllWhere!(User, "age > ?")(20).array;
		auto total = fold!((a,b) => User("", a.age + b.age))(users);

		assert(total.age == 55 + 91 + 27);
		assert(db.selectOneWhere!(User, "age == ?")(27).name == "maria");
		assert(db.selectRow!User(2).age == 91);

	};

	int insert(OR OPTION = OR.None, T)(T row) {
		if(autoCreateTable && !hasTable(SQLName!T)) {
			if(!create!T)
				return 0;
		}
		super.insert!OPTION(row).step();
		return db.changes;
	}

	unittest {
		mixin TEST;
		User user = { "jonas", 45 };
		assert(db.insert(user));
		assert(db.query("select name from User where age = 45").step());
		assert(!db.query("select age from User where name = 'xxx'").step());
	};

	int delWhere(T, string expr, ARGS...)(ARGS args) if(expr.length) {
		super.query(SB.del!T.where(expr), args).step();
		return db.changes;
	}

	unittest {
		mixin TEST;
		User user = { "jonas", 45 };
		assert(db.insert(user));
		assert(db.delWhere!(User, "age = ?")(45));
	}
}

unittest
{
	// Test quoting by using keyword as table and column name
	mixin TEST;
	struct Group { int Group; }

	Group a = { 3 };
	db.insert(a);
	Group b = db.selectOneWhere!(Group, `"Group"=3`);
	assert(a.Group == b.Group);
}