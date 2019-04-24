package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.ShortReader;
import clojure.lang.RT;


public interface ShortTensorReader extends ShortReader
{
  short read2d(int row, int col);
  short tensorRead(IntIter dims);
  default Object invoke(Object row, Object col) {
    return read2d(RT.intCast(row), RT.intCast(col));
  }
}
