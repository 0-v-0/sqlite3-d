module sqlite3_d.database;

import 
	etc.c.sqlite3,
	sqlite3_d;

/// Setup code for tests
version(unittest) template TEST(string dbname)
{
	struct User {
		string name;
		int age;
	};

	struct Message {
		@as("rowid") int id;
		string content;
		int byUser;
	};

	Database db = () {
		tryRemove(dbname ~ ".db");
		return new Database(dbname ~ ".db");
	}();
}

// Returned from select-type methods where the row type is known
struct QueryIterator(T)
{
	Query query;
	this(Query q) { query = q; }

	bool empty() {
		import etc.c.sqlite3;

		if(query.lastCode < 0)
			query.step();
		return query.lastCode != SQLITE_ROW;
	}
	void popFront() { query.step(); }
	T front() { return query.get!T; }
}

unittest {
	QueryIterator!int qi;
	assert(qi.empty());
}
/// An Database with query building capabilities
class Database : SQLite3
{
	bool autoCreateTable = true;

	alias SQLBuilder SB;

	this(string name) { super(name); }

	bool create(T)()
	{
		auto q = Query(db, SB.create!T);
		q.step();
		return q.lastCode == SQLITE_DONE;
	}

	QueryIterator!T selectAllWhere(T, string WHERE, ARGS...)(ARGS args)
	{
		auto q = Query(db, SB.selectAllFrom!T.where(WHERE), args);
		return QueryIterator!T(q);
	}

	T selectOneWhere(T, string WHERE, ARGS...)(ARGS args)
	{
		auto q = Query(db, SB.selectAllFrom!T.where(WHERE), args);
		if(q.step())
			return q.get!T;
		throw new SQLiteException("No match");
	}

	T selectOneWhere(T, string WHERE, T defValue = T.init, ARGS...)(ARGS args) {
		auto q = Query(db, SB.selectAllFrom!T.where(WHERE), args);
		if (q.step())
			return q.get!T;
		return defValue;
	}

	T selectRow(T)(ulong row)
	{
		return selectOneWhere!(T, "rowid=?")(row);
	}

	unittest {
		mixin TEST!"select";
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
				return false;
		}
		auto q = Query.insert!OPTION(db, row);
		q.step();
		return q.changes;
	}

	unittest {
		mixin TEST!"insert";
		User user = { "jonas", 45 };
		assert(db.insert(user));
		assert(db.query("select name from User where age = 45").step());
		assert(!db.query("select age from User where name = 'xxx'").step());
	};
}

unittest
{
	// Test quoting by using keyword as table and column name
	mixin TEST!"testdb";
	struct Group {
		int Group;
	}

	Group g = { 3 };
	db.insert(g);
	Group gg = db.selectOneWhere!(Group, `"Group"=3`);
	assert(gg.Group == g.Group);
}