package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.DoubleWriter;
import clojure.lang.RT;


public interface DoubleTensorWriter extends DoubleWriter
{
  void write2d(long row, long col, double val);
  void write3d(long height, long width, long chan, double val);
  void tensorWrite(Iterable dims, double val);
  default Object invoke(Object row, Object col, Object val) {
    write2d(RT.longCast(row), RT.longCast(col), RT.doubleCast(val));
    return null;
  }
  default Object invoke(Object row, Object col, Object chan, Object val) {
    write3d(RT.longCast(row), RT.longCast(col), RT.longCast(chan), RT.doubleCast(val));
    return null;
  }
}
