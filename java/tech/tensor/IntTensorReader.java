package tech.tensor;

import tech.datatype.IntIter;
import tech.datatype.IntReader;


public interface IntTensorReader extends IntReader
{
  int read2d(int row, int col);
  int tensorRead(IntIter dims);
  default Object invoke(Object row, Object col) {
    return read2d((int) row, (int) col);
  }
}
