package tech.tensor;

import tech.datatype.IntIter;


public interface LongReader
{
  long read2d(int row, int col);
  long tensorRead(IntIter dims);
}
