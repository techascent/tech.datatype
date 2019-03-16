package tech.datatype;

import java.nio.*;

public interface ShortWriter
{
  void write(int idx, short value);
  void writeBlock(int offset, ShortBuffer values);
  void writeIndexes(IntBuffer indexes, ShortBuffer values);
}
