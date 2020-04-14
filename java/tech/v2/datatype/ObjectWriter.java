package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.RT;


public interface ObjectWriter extends IOBase, IFn
{
  void write(long idx, Object value);
  default Object getDatatype () { return Keyword.intern(null, "object"); }
  default Object invoke(Object idx, Object value)
  {
    write( RT.longCast(idx),  value);
    return null;
  }
};
