package tech.tensor;

import tech.datatype.IntIter;
import tech.datatype.DoubleReader;


public interface DoubleTensorReader extends DoubleReader
{
  double read2d(int row, int col);
  double tensorRead(IntIter dims);
  default Object invoke(Object row, Object col) {
    return read2d((int) row, (int) col);
  }
}
