package tech.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;


public interface ByteWriter extends IOBase, IFn
{
  void write(long idx, byte value);
  default Keyword getDatatype () { return Keyword.intern(null, "int8"); }
  default Object invoke(Object idx, Object value)
  {
    write((long) idx, (byte)value);
    return null;
  }
}
