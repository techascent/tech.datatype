package tech.datatype;

import java.nio.*;

public interface LongWriter extends Datatype
{
  void write(int idx, long value);
  void writeConstant(int idx, long value, int count);
  void writeBlock(int offset, LongBuffer values);
  void writeIndexes(IntBuffer indexes, LongBuffer values);
};
