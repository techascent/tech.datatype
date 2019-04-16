package tech.tensor;

import tech.datatype.IntIter;


public interface ByteReader
{
  byte read2d(int row, int col);
  byte tensorRead(IntIter dims);
}
