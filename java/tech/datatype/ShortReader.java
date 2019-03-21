package tech.datatype;

import java.nio.*;

public interface ShortReader extends Datatype
{
  short read(int idx);
  void readBlock(int offset, ShortBuffer destination);
  void readIndexes(IntBuffer indexes, ShortBuffer destination);
}
