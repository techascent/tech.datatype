package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.RT;


public interface LongWriter extends IOBase, IFn
{
  void write(long idx, long value);
  default Object getDatatype () { return Keyword.intern(null, "int64"); }
  default Object invoke(Object idx, Object value)
  {
    write(RT.longCast(idx), RT.longCast(value));
    return null;
  }
};
