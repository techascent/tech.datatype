package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.ObjectWriter;
import clojure.lang.RT;


public interface ObjectTensorWriter extends ObjectWriter
{
  void write2d(long row, long col, Object value);
  void write3d(long height, long width, long chan, Object value);
  void tensorWrite(Iterable dims, Object value);
  default Object invoke(Object row, Object col, Object val) {
    write2d(RT.longCast(row), RT.longCast(col), val);
    return null;
  }
  default Object invoke(Object row, Object col, Object chan, Object val) {
    write3d(RT.longCast(row), RT.longCast(col), RT.longCast(chan), val);
    return null;
  }
}
