package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.IntWriter;
import clojure.lang.RT;


public interface IntTensorWriter extends IntWriter
{
  void write2d(long row, long col, int val);
  void write3d(long height, long width, long chan, int val);
  void tensorWrite(Iterable dims, int val);
  default Object invoke(Object row, Object col, Object val) {
    write2d(RT.intCast(row), RT.intCast(col), RT.intCast(val));
    return null;
  }
  default Object invoke(Object row, Object col, Object chan, Object val) {
    write3d(RT.longCast(row), RT.longCast(col), RT.longCast(chan), RT.intCast(val));
    return null;
  }
}
