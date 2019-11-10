package tech.v2.tensor;

import tech.v2.datatype.ObjectReader;
import clojure.lang.RT;


public interface ObjectTensorReader extends ObjectReader
{
  Object read2d(long row, long col);
  Object read3d(long height, long width, long chan);
  Object tensorRead(Iterable dims);
  default Object invoke(Object row, Object col) {
    return read2d(RT.longCast(row), RT.longCast(col));
  }
  default Object invoke(Object row, Object col, Object chan) {
    return read3d(RT.longCast(row), RT.longCast(col), RT.longCast(chan));
  }
}
