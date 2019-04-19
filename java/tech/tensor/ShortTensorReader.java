package tech.tensor;

import tech.datatype.IntIter;
import tech.datatype.ShortReader;


public interface ShortTensorReader extends ShortReader
{
  short read2d(int row, int col);
  short tensorRead(IntIter dims);
  default Object invoke(Object row, Object col) {
    return read2d((int) row, (int) col);
  }
}
