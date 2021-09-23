module sqlite3_d.database;

import sqlite3_d;

/// Setup code for tests
version(unittest) mixin template TEST(string dbname)
{
	struct User {
		string name = "";
		int age;
	};

	struct Message {
		@sqlname("rowid") int id;
		string content;
		int byUser;
	};

	Database db = () {
		tryRemove(dbname ~ ".db");
		return new Database(dbname ~ ".db");
	}();
}

/// An Database with query building capabilities
class Database : SQLite3
{
	bool autoCreateTable = true;

	alias QB = QueryBuilder!();
	// Returned from select-type methods where the row type is known
	struct QueryIterator(T)
	{
		Query query;
		bool finished;
		this(Query q)
		{
			query = q;
			finished = !query.step();
		}

		bool empty() { return finished; }
		void popFront() { finished = !query.step(); }
		T front() { return query.get!T(); }
	}

	this(string name) { super(name); }

	bool create(T)()
	{
		return Query(db, QB.create!T()).step();
	}

	QueryIterator!T selectAllWhere(T, string WHERE, ARGS...)(ARGS args)
	{
		auto q = Query(db, QB.selectAllFrom!T.where!WHERE(args));
		q.bind(args);
		return QueryIterator!T(q);
	}

	T selectOneWhere(T, string WHERE, ARGS...)(ARGS args)
	{
		auto q = Query(db, QB.selectAllFrom!T().where!WHERE(args));
		q.bind(args);
		if(q.step())
			return q.get!T();
		throw new db_exception("No match");
	}

	T selectRow(T)(ulong row)
	{
		return selectOneWhere!(T, "rowid=?")(row);
	}

	unittest {
		mixin TEST!("select");
		import std.array : array;
		import std.algorithm.iteration : fold;

		db.create!User();
		db.insert(User("jonas", 55));
		db.insert(User("oliver", 91));
		db.insert(User("emma", 12));
		db.insert(User("maria", 27));

		User[] users = array(db.selectAllWhere!(User, "age > ?")(20));
		auto total = fold!((a,b) => User("", a.age + b.age))(users);

		assert(total.age == 55 + 91 + 27);
		assert(db.selectOneWhere!(User, "age == ?")(27).name == "maria");
		assert(db.selectRow!User(2).age == 91);

	};

	bool insert(OR OPTION = OR.None, T)(T row)
	{
		auto qb = QB.insert!OPTION(row);
		Query q;
		if(autoCreateTable) {
			try {
				q = Query(db, qb);
			} catch(db_exception dbe) {
				if(hasTable(TableName!T))
					return false;
				create!T();
				q = Query(db, qb);
			}
		} else
			q = Query(db, qb);

		q.bind(qb.binds.expand);
		return q.step();
	}

	unittest {
		mixin TEST!("insert");
		User user = { "jonas", 45 };
		db.insert(user);
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

	db.create!Group();
	Group g = { 3 };
	db.insert(g);
	Group gg = db.selectOneWhere!(Group, "\"Group\"=3");
	assert(gg.Group == g.Group);
}