package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.DoubleReader;
import clojure.lang.RT;


public interface DoubleTensorReader extends DoubleReader
{
  double read2d(long row, long col);
  double read3d(long height, long width, long chan);
  double tensorRead(Iterable dims);
  default Object invoke(Object row, Object col) {
    return read2d(RT.longCast(row), RT.longCast(col));
  }
  default Object invoke(Object row, Object col, Object chan) {
    return read3d(RT.longCast(row), RT.longCast(col), RT.longCast(chan));
  }
}
