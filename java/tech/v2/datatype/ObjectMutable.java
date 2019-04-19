package tech.v2.datatype;

import clojure.lang.Keyword;

public interface ObjectMutable extends MutableRemove
{
  void insert(long idx, Object value);
  void append(Object value);

  default Keyword getDatatype () { return Keyword.intern(null, "object"); }
}
