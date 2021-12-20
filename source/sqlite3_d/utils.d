module sqlite3_d.utils;

import
	std.array,
	std.meta,
	std.string,
	std.traits;
import std.uni : isWhite;

struct as { string name; }
struct sqlkey { string key; }
struct sqltype { string type; }

package:

alias
	getAttr(T...) = __traits(getAttributes, T),
	toz = toStringz;

auto toStr(T)(T ptr) {
	return fromStringz(ptr).idup;
}

/// Try to remove 'name', return true on success
bool tryRemove(string name) {
	import std.file;
	try {
		std.file.remove(name);
	} catch (FileException e) {
		return false;
	}
	return true;
}

S quote(S)(S s, S q = "'") pure nothrow if (isSomeString!S) {
	return q ~ s ~ q;
}

S quoteJoin(S, bool leaveTail = false)(S[] s, S sep = ",", S q = "'") pure nothrow if (isSomeString!S) {
	auto res = appender!(S);
	for(size_t i; i < s.length; i++) {
		res ~= q;
		res ~= s[i];
		res ~= q;
		if (leaveTail || i+1 < s.length)
			res ~= sep;
	}
	return res[];
}

template startsWithWhite(alias S) {
	static if(is(typeof(S) : string))
		static if(S.length)
			static if(S[0].isWhite)
				enum startsWithWhite = true;
	static if(!is(typeof(startsWithWhite) == bool))
		enum startsWithWhite = false;
}

enum
	allString(T...) = allSatisfy!(isSomeString, T),
	allAggregate(T...) = allSatisfy!(isAggregateType, T);

alias CutOut(size_t I, T...) = AliasSeq!(T[0..I], T[I+1..$]);
