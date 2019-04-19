package tech.tensor;

import tech.datatype.IntIter;
import tech.datatype.FloatReader;

public interface FloatTensorReader extends FloatReader
{
  float read2d(int row, int col);
  float tensorRead(IntIter dims);
  default Object invoke(Object row, Object col) {
    return read2d((int) row, (int) col);
  }
}
