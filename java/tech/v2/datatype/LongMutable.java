package tech.v2.datatype;

import clojure.lang.Keyword;

public interface LongMutable extends MutableRemove
{
  default Object getDatatype () { return Keyword.intern(null, "int64"); }
  void insert(long idx, long value);
  default void append(long value)
  {
    insert(lsize(), value);
  }
}
