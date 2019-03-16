package tech.datatype;

import java.nio.*;

public interface FloatWriter
{
  float write(int idx, float value);
  void writeBlock(int offset, FloatBuffer values);
  void writeIndexes(IntBuffer indexes, FloatBuffer values);
};
