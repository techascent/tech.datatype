package tech.tensor;

import tech.datatype.IntIter;
import tech.datatype.BooleanReader;


public interface BooleanTensorReader extends BooleanReader
{
  boolean read2d(int row, int col);
  boolean tensorRead(IntIter dims);
  default Object invoke(Object row, Object col) {
    return read2d((int) row, (int) col);
  }
}
