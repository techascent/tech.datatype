package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.ByteReader;
import clojure.lang.RT;


public interface ByteTensorReader extends ByteReader
{
  byte read2d(long row, long col);
  byte read3d(long height, long width, long chan);
  byte tensorRead(Iterable dims);
  default Object invoke(Object row, Object col) {
    return read2d(RT.longCast(row), RT.longCast(col));
  }
}
