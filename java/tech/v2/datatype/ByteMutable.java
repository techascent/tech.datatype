package tech.v2.datatype;

import clojure.lang.Keyword;

public interface ByteMutable extends MutableRemove
{
  default Object getDatatype () { return Keyword.intern(null, "int8"); }
  void insert(long idx, byte value);
  default void append(byte value)
  {
    insert(lsize(), value);
  }
}
