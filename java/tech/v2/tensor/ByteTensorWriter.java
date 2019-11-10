package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.ByteWriter;
import clojure.lang.RT;


public interface ByteTensorWriter extends ByteWriter
{
  void write2d(long row, long col, byte val);
  void write3d(long height, long width, long chan, byte val);
  void tensorWrite(Iterable dims, byte val);
  default Object invoke(Object row, Object col, Object val) {
    write2d(RT.longCast(row), RT.longCast(col), RT.byteCast(val));
    return null;
  }
  default Object invoke(Object row, Object col, Object chan, Object val) {
    write3d(RT.longCast(row), RT.longCast(col), RT.longCast(chan), RT.byteCast(val));
    return null;
  }
}
