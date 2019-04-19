package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;


public interface IntWriter extends IOBase, IFn
{
  void write(long idx, int value);
  default Keyword getDatatype () { return Keyword.intern(null, "int32"); }
  default Object invoke(Object idx, Object value)
  {
    write((long) idx, (int) value);
    return null;
  }
}
