package tech.tensor;

import tech.datatype.IntIter;


public interface ShortReader
{
  short read2d(int row, int col);
  short tensorRead(IntIter dims);
}
