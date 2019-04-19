package tech.v2.datatype;

import clojure.lang.Keyword;

public interface LongMutable extends MutableRemove
{
  void insert(long idx, long value);
  void append(long value);
  default Keyword getDatatype () { return Keyword.intern(null, "int64"); }
}
