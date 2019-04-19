package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;


public interface ShortWriter extends IOBase, IFn
{
  void write(long idx, short value);
  default Keyword getDatatype () { return Keyword.intern(null, "int16"); }
  default Object invoke(Object idx, Object value)
  {
    write((long) idx, (short) value);
    return null;
  }
}
