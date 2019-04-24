package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.RT;


public interface DoubleWriter extends IOBase, IFn
{
  void write(long idx, double value);
  default Keyword getDatatype () { return Keyword.intern(null, "float64"); }
  default Object invoke(Object idx, Object value)
  {
    write( RT.longCast(idx), RT.doubleCast(value));
    return null;
  }
}
