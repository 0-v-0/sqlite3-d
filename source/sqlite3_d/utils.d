module sqlite3_d.utils;

struct sqlname { string name; }
struct sqlkey { string key; }
struct sqltype { string type; }

package:

alias getAttr(T...) = __traits(getAttributes, T);
alias getMember(alias I, alias N) = __traits(getMember, I, N);


/// Try to remove 'name', return true on success
debug bool tryRemove(string name) {
	import std.file;
	try {
		std.file.remove(name);
	} catch (FileException e) {
		return false;
	}
	return true;
}


static string quote(string s, string q = "'") pure nothrow 
{
	return q ~ s ~ q;
}

static string[] quote(string[] s, string q = "'") pure nothrow 
{
	string[] res;
	foreach(t ; s)
		res ~= q ~ t ~ q;
	return res;
}

import std.traits;

@property bool allString(STRING...)() {
	bool ok = true;
	foreach(S ; STRING)
		static if(is(S))
		ok = false;
	else
		ok &= isSomeString!(typeof(S));
	return ok;
}

@property bool allAggregate(ARGS...)() {
	bool ok = true;
	foreach(A ; ARGS)
		static if(is(A))
		ok &= isAggregateType!A;
	else
		ok = false;
	return ok;
}
