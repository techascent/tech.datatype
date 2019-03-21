package tech.datatype;

import java.nio.*;

public interface ShortWriter extends Datatype
{
  void write(int idx, short value);
  void writeConstant(int idx, short value, int count);
  void writeBlock(int offset, ShortBuffer values);
  void writeIndexes(IntBuffer indexes, ShortBuffer values);
}
