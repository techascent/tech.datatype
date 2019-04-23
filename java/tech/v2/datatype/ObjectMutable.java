package tech.v2.datatype;

import clojure.lang.Keyword;

public interface ObjectMutable extends MutableRemove
{
  default Keyword getDatatype () { return Keyword.intern(null, "object"); }
  void insert(long idx, Object value);
  default void append(Object value)
  {
    insert(lsize(), value);
  }
}
