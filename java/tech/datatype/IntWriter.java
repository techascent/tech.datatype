package tech.datatype;

import java.nio.*;

public interface IntWriter
{
  void write(int idx, int value);
  void writeBlock(int offset, IntBuffer values);
  void writeIndexes(IntBuffer indexes, IntBuffer values);
}
