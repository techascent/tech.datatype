package tech.datatype;

import java.nio.*;

public interface LongReader
{
  long read(int idx);
  void readBlock(int offset, LongBuffer destination);
  void readIndexes(IntBuffer indexes, LongBuffer destination);
}
