package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;


public interface LongWriter extends IOBase, IFn
{
  void write(long idx, long value);
  default Keyword getDatatype () { return Keyword.intern(null, "int64"); }
  default Object invoke(Object idx, Object value)
  {
    write((long) idx, (long) value);
    return null;
  }
};
