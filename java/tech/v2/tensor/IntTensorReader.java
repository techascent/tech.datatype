package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.IntReader;
import clojure.lang.RT;


public interface IntTensorReader extends IntReader
{
  int read2d(int row, int col);
  int tensorRead(IntIter dims);
  default Object invoke(Object row, Object col) {
    return read2d(RT.intCast(row), RT.intCast(col));
  }
}
