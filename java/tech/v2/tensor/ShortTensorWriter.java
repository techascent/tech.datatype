package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.ShortWriter;
import clojure.lang.RT;


public interface ShortTensorWriter extends ShortWriter
{
  void write2d(long row, long col, short value);
  void write3d(long height, long width, long chan, short value);
  void tensorWrite(Iterable dims, short value);
  default Object invoke(Object row, Object col, Object val) {
    write2d(RT.longCast(row), RT.longCast(col), RT.shortCast(val));
    return null;
  }
  default Object invoke(Object row, Object col, Object chan, Object val) {
    write3d(RT.longCast(row), RT.longCast(col), RT.longCast(chan), RT.shortCast(val));
    return null;
  }
}
