package tech.tensor;

import tech.datatype.IntIter;
import tech.datatype.LongReader;


public interface LongTensorReader extends LongReader
{
  long read2d(int row, int col);
  long tensorRead(IntIter dims);
  default Object invoke(Object row, Object col) {
    return read2d((int) row, (int) col);
  }
}
