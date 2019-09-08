package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.BooleanReader;
import clojure.lang.RT;


public interface BooleanTensorReader extends BooleanReader
{
  boolean read2d(long row, long col);
  boolean tensorRead(Iterable dims);
  default Object invoke(Object row, Object col) {
    return read2d(RT.intCast(row), RT.intCast(col));
  }
}
