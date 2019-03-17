package tech.datatype;

import java.nio.*;

public interface DoubleWriter
{
  void write(int idx, double value);
  void writeConstant(int idx, double value, int count);
  void writeBlock(int offset, DoubleBuffer values);
  void writeIndexes(IntBuffer indexes, DoubleBuffer values);
}
