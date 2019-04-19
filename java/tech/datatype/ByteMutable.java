package tech.datatype;

import clojure.lang.Keyword;

public interface ByteMutable extends MutableRemove
{
  default Keyword getDatatype () { return Keyword.intern(null, "int8"); }
  void insert(long idx, byte value);
  void append(byte value);
}
