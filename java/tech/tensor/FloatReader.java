package tech.tensor;

import tech.datatype.IntIter;


public interface FloatReader
{
  float read2d(int row, int col);
  float tensorRead(IntIter dims);
}
