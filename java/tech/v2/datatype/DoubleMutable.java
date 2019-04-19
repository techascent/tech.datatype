package tech.v2.datatype;

import clojure.lang.Keyword;


public interface DoubleMutable extends MutableRemove
{
  void insert(long idx, double value);
  void append(double value);
  default Keyword getDatatype () { return Keyword.intern(null, "float64"); }
}
