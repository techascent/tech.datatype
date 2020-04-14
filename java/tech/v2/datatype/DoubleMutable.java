package tech.v2.datatype;

import clojure.lang.Keyword;


public interface DoubleMutable extends MutableRemove
{
  default Object getDatatype () { return Keyword.intern(null, "float64"); }
  void insert(long idx, double value);
  default void append(double value)
  {
    insert(lsize(), value);
  }
}
