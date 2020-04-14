package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.RT;


public interface BooleanWriter extends IOBase, IFn
{
  void write(long idx, boolean value);
  default Object getDatatype () { return Keyword.intern(null, "boolean"); }
  default Object invoke(Object idx, Object value)
  {
    write( RT.longCast(idx), RT.booleanCast(value));
    return null;
  }
}
