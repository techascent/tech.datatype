package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;


public interface BooleanWriter extends IOBase, IFn
{
  void write(long idx, boolean value);
  default Keyword getDatatype () { return Keyword.intern(null, "boolean"); }
  default Object invoke(Object idx, Object value)
  {
    write( (long) idx, (boolean) value);
    return null;
  }
}
