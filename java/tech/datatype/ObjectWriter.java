package tech.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;


public interface ObjectWriter extends IOBase, IFn
{
  void write(long idx, Object value);
  default Keyword getDatatype () { return Keyword.intern(null, "object"); }
  default Object invoke(Object idx, Object value)
  {
    write( (long) idx,  value);
    return null;
  }
};
