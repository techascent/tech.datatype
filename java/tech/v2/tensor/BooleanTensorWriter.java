package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.BooleanWriter;
import clojure.lang.RT;


public interface BooleanTensorWriter extends BooleanWriter
{
  boolean write2d(long row, long col, boolean value);
  boolean write3d(long height, long width, long chan, boolean value);
  boolean tensorWrite(Iterable dims, boolean value);
  default Object invoke(Object row, Object col, Object value) {
    return write2d(RT.longCast(row), RT.longCast(col), RT.booleanCast(value));
  }
  default Object invoke(Object row, Object col, Object chan, Object value) {
    return write3d(RT.longCast(row), RT.longCast(col), RT.longCast(chan), RT.booleanCast(value));
  }
}
