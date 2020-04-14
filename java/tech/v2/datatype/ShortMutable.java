package tech.v2.datatype;

import clojure.lang.Keyword;

public interface ShortMutable extends MutableRemove
{
  default Object getDatatype () { return Keyword.intern(null, "int16"); }
  void insert(long idx, short value);
  default void append(short value)
  {
    insert(lsize(), value);
  }
}
