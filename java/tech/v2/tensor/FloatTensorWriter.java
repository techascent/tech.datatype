package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.FloatWriter;
import clojure.lang.RT;


public interface FloatTensorWriter extends FloatWriter
{
  void write2d(long row, long col, float val);
  void write3d(long height, long width, long chan, float val);
  void tensorWrite(Iterable dims, float val);
  default Object invoke(Object row, Object col, Object val) {
    write2d(RT.longCast(row), RT.longCast(col), RT.floatCast(val));
    return null;
  }
  default Object invoke(Object row, Object col, Object chan, Object val) {
    write3d(RT.longCast(row), RT.longCast(col), RT.longCast(chan), RT.floatCast(val));
    return null;
  }
}
