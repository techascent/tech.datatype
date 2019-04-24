package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.DoubleReader;
import clojure.lang.RT;


public interface DoubleTensorReader extends DoubleReader
{
  double read2d(int row, int col);
  double tensorRead(IntIter dims);
  default Object invoke(Object row, Object col) {
    return read2d(RT.intCast(row), RT.intCast(col));
  }
}
