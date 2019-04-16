package tech.tensor;

import tech.datatype.IntIter;


public interface ObjectReader
{
  Object read2d(int row, int col);
  Object tensorRead(IntIter dims);
}
