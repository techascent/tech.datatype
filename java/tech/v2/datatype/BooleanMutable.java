package tech.v2.datatype;

import clojure.lang.Keyword;

public interface BooleanMutable extends MutableRemove
{
  default Keyword getDatatype () { return Keyword.intern(null, "boolean"); }
  void insert(long idx, boolean value);
  default void append(boolean value)
  {
    insert(lsize(), value);
  }
}
