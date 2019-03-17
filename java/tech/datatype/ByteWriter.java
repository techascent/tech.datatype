package tech.datatype;

import java.nio.*;

public interface ByteWriter
{
  void write(int idx, byte value);
  void writeConstant(int idx, byte value, int count);
  void writeBlock(int offset, ByteBuffer values);
  void writeIndexes(IntBuffer indexes, ByteBuffer values);
}
