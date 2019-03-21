package tech.datatype;

import java.nio.*;

public interface DoubleReader extends Datatype
{
  double read(int idx);
  void readBlock(int offset, DoubleBuffer destination);
  void readIndexes(IntBuffer indexes, DoubleBuffer destination);
}
