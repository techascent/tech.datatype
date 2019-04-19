package tech.tensor;


import tech.datatype.IntIter;
import tech.datatype.ByteReader;


public interface ByteTensorReader extends ByteReader
{
  byte read2d(int row, int col);
  byte tensorRead(IntIter dims);
  default Object invoke(Object row, Object col) {
    return read2d((int) row, (int) col);
  }
}
