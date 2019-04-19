package tech.v2.datatype;

import clojure.lang.Keyword;

public interface IntMutable extends MutableRemove
{
  void insert(long idx, int value);
  void append(int value);
  default Keyword getDatatype () { return Keyword.intern(null, "int32"); }
}
