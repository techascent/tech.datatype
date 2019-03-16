package tech.datatype;

import java.nio.*;

public interface DoubleWriter
{
  double write(int idx, double value);
  void writeBlock(int offset, DoubleBuffer values);
  void writeIndexes(IntBuffer indexes, DoubleBuffer values);
}
