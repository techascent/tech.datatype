package tech.datatype;

import java.nio.*;

public interface IntReader extends Datatype
{
  int read(int idx);
  void readBlock(int offset, IntBuffer destination);
  void readIndexes(IntBuffer indexes, IntBuffer destination);
}
