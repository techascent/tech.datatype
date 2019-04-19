package tech.datatype;

import clojure.lang.Keyword;

public interface LongMutable extends MutableRemove
{
  void insert(long idx, long value);
  void append(long value);
  default Keyword getDatatype () { return Keyword.intern(null, "int64"); }
}
