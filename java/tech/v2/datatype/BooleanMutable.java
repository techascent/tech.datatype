package tech.v2.datatype;

import clojure.lang.Keyword;

public interface BooleanMutable extends MutableRemove
{
  void insert(long idx, boolean value);
  void append(boolean value);
  default Keyword getDatatype () { return Keyword.intern(null, "boolean"); }
}
