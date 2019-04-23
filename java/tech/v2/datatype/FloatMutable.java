package tech.v2.datatype;

import clojure.lang.Keyword;

public interface FloatMutable extends MutableRemove
{
  default Keyword getDatatype () { return Keyword.intern(null, "float32"); }
  void insert(long idx, float value);
  default void append(float value)
  {
    insert(lsize(), value);
  }
}
