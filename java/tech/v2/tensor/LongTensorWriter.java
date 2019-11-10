package tech.v2.tensor;

import tech.v2.datatype.IntIter;
import tech.v2.datatype.LongWriter;
import clojure.lang.RT;


public interface LongTensorWriter extends LongWriter
{
  long write2d(long row, long col, long val);
  long write3d(long height, long width, long chan, long val);
  long tensorWrite(Iterable dims, long val);
  default Object invoke(Object row, Object col, Object val) {
    write2d(RT.longCast(row), RT.longCast(col), RT.longCast(val));
    return null;
  }
  default Object invoke(Object row, Object col, Object chan, Object val) {
    write3d(RT.longCast(row), RT.longCast(col), RT.longCast(chan), RT.longCast(val));
    return null;
  }
}
