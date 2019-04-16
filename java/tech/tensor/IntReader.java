package tech.tensor;

import tech.datatype.IntIter;


public interface IntReader
{
  int read2d(int row, int col);
  int tensorRead(IntIter dims);
}
