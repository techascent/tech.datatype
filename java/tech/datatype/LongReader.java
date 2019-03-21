package tech.datatype;

import java.nio.*;

public interface LongReader extends Datatype
{
  long read(int idx);
  void readBlock(int offset, LongBuffer destination);
  void readIndexes(IntBuffer indexes, LongBuffer destination);
}
