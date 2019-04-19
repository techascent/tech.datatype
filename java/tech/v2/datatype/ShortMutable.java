package tech.v2.datatype;

import clojure.lang.Keyword;

public interface ShortMutable extends MutableRemove
{
  void insert(long idx, short value);
  void append(short value);
  default Keyword getDatatype () { return Keyword.intern(null, "int16"); }
}
