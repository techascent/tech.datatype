package tech.tensor;

import tech.datatype.IntIter;
import tech.datatype.ObjectReader;


public interface ObjectTensorReader extends ObjectReader
{
  Object read2d(int row, int col);
  Object tensorRead(IntIter dims);
  default Object invoke(Object row, Object col) {
    return read2d((int) row, (int) col);
  }
}
