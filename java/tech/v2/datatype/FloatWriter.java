package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;


public interface FloatWriter extends IOBase, IFn
{
  void write(long idx, float value);
  default Keyword getDatatype () { return Keyword.intern(null, "float32"); }
  default Object invoke(Object idx, Object value)
  {
    write( (long) idx, (float) value);
    return null;
  }
};
