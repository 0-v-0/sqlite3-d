module sqlite3_d.utils;

import
	std.array,
	std.ascii,
	std.meta,
	std.string;
import std.uni : isWhite;
package import std.traits;

enum {
	default0 = "default '0'",
	notnull = "not null",
	unique = "unique"
}

struct as { string name; }
struct sqlkey { string key; }
struct sqltype { string type; }
struct ignore;
struct uncamel;

enum CharClass {
	Other,
	LowerCase,
	UpperCase,
	Underscore,
	Digit,
}

CharClass classify(char ch) pure {
	with (CharClass) {
		if (isLower(ch)) return LowerCase;
		if (isUpper(ch)) return UpperCase;
		if (isDigit(ch)) return Digit;
		if (ch == '_') return Underscore;
		return Other;
	}
}

S unCamelCase(S)(S s, char sep = '_') pure {
 	if (!s.length)
 		return "";
	char[128] buffer;
	size_t length;

	auto pcls = classify(s[0]);
	foreach (ch; s) {
		auto cls = classify(ch);
		switch (cls) with (CharClass) {
		case UpperCase:
			if (pcls != UpperCase && pcls != Underscore)
				buffer[length++] = sep;
			buffer[length++] = ch | ' ';
			break;
		case Digit:
			if (pcls != Digit)
				buffer[length++] = sep;
			goto default;
		default:
			buffer[length++] = ch;
			break;
		}
		pcls = cls;

		if (length >= buffer.length-1)
			break;
	}
	return cast(S)buffer[0..length].dup;
}

unittest {
	void test(string str, string expected) {
		auto result = str.unCamelCase;
		assert(result == expected, str ~ ": " ~ result);
	}
	test("AA", "aa");
	test("AaA", "aa_a");
	test("AaA1", "aa_a_1");
	test("AaA11", "aa_a_11");
	test("_AaA1", "_aa_a_1");
	test("_AaA11_", "_aa_a_11_");
	test("aaA", "aa_a");
	test("aaAA", "aa_aa");
	test("aaAA1", "aa_aa_1");
	test("aaAA11", "aa_aa_11");
	test("authorName", "author_name");
	test("authorBio", "author_bio");
	test("authorPortraitId", "author_portrait_id");
	test("authorPortraitID", "author_portrait_id");
	test("coverURL", "cover_url");
	test("coverImageURL", "cover_image_url");
}

package:

bool startsWithWhite(S)(S s) if(isArray!S) {
	return s.length && s[0].isWhite;
}

template getSQLFields(string prefix, string suffix, T) {
	enum
		colNames = ColumnNames!T,
		I = staticIndexOf!("rowid", colNames),
		sql(S...) = prefix ~ [S].quoteJoin(suffix == "=?" ? "=?," : ",")
			~ suffix;
	// Skips "rowid" field
	static if(I >= 0)
		enum sqlFields = CutOut!(I, colNames);
	else
		enum sqlFields = colNames;
}

template ExceptionThis() {
	this(string msg, string file = __FILE__, size_t line = __LINE__) { super(msg, file, line); }
	this(Throwable causedBy, string f = __FILE__, size_t l = __LINE__) { super(causedBy.msg, causedBy, f, l); }
	this(string msg, Throwable causedBy, string f = __FILE__, size_t l = __LINE__) { super(causedBy.msg, causedBy, f, l); }
}

alias
	CutOut(size_t I, T...) = AliasSeq!(T[0..I], T[I+1..$]),
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
