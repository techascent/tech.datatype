package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.FloatReader;
import clojure.lang.RT;


public interface FloatTensorReader extends FloatReader
{
  float read2d(long row, long col);
  float tensorRead(Iterable dims);
  default Object invoke(Object row, Object col) {
    return read2d(RT.intCast(row), RT.intCast(col));
  }
}
