package tech.tensor;

import tech.datatype.IntIter;


public interface BooleanReader
{
  boolean read2d(int row, int col);
  boolean tensorRead(IntIter dims);
}
