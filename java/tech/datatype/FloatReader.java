package tech.datatype;

import java.nio.*;

public interface FloatReader extends Datatype
{
  float read(int idx);
  void readBlock(int offset, FloatBuffer destination);
  void readIndexes(IntBuffer indexes, FloatBuffer destination);
}
