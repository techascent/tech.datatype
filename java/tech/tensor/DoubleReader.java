package tech.tensor;

import tech.datatype.IntIter;


public interface DoubleReader
{
  double read2d(int row, int col);
  double tensorRead(IntIter dims);
}
