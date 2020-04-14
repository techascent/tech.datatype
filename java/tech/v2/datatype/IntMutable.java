package tech.v2.datatype;

import clojure.lang.Keyword;

public interface IntMutable extends MutableRemove
{
  default Object getDatatype () { return Keyword.intern(null, "int32"); }
  void insert(long idx, int value);
  default void append(int value)
  {
    insert(lsize(), value);
  }
}
