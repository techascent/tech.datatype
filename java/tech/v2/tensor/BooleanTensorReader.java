package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.BooleanReader;
import clojure.lang.RT;


public interface BooleanTensorReader extends BooleanReader
{
  boolean read2d(long row, long col);
  boolean read3d(long height, long width, long chan);
  boolean tensorRead(Iterable dims);
  default Object invoke(Object row, Object col) {
    return read2d(RT.longCast(row), RT.longCast(col));
  }
  default Object invoke(Object row, Object col, Object chan) {
    return read3d(RT.longCast(row), RT.longCast(col), RT.longCast(chan));
  }
}
