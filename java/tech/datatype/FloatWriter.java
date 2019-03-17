package tech.datatype;

import java.nio.*;

public interface FloatWriter
{
  void write(int idx, float value);
  void writeConstant(int idx, float value, int count);
  void writeBlock(int offset, FloatBuffer values);
  void writeIndexes(IntBuffer indexes, FloatBuffer values);
};
