package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.RT;


public interface ShortWriter extends IOBase, IFn
{
  void write(long idx, short value);
  default Keyword getDatatype () { return Keyword.intern(null, "int16"); }
  default Object invoke(Object idx, Object value)
  {
    write(RT.longCast(idx), RT.shortCast(value));
    return null;
  }
}
