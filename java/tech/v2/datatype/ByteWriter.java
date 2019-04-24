package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.RT;


public interface ByteWriter extends IOBase, IFn
{
  void write(long idx, byte value);
  default Keyword getDatatype () { return Keyword.intern(null, "int8"); }
  default Object invoke(Object idx, Object value)
  {
    write( RT.longCast( idx ), RT.byteCast( value) );
    return null;
  }
}
