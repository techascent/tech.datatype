package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.ObjectReader;
import clojure.lang.RT;


public interface ObjectTensorReader extends ObjectReader
{
  Object read2d(int row, int col);
  Object tensorRead(IntIter dims);
  default Object invoke(Object row, Object col) {
    return read2d(RT.intCast(row), RT.intCast(col));
  }
}
