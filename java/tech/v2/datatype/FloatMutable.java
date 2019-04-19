package tech.v2.datatype;

import clojure.lang.Keyword;

public interface FloatMutable extends MutableRemove
{
  void insert(long idx, float value);
  void append(float value);
  default Keyword getDatatype () { return Keyword.intern(null, "float32"); }
}
