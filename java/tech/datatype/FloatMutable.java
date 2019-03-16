package tech.datatype;
import java.nio.*;


public interface FloatMutable
{
  void insert(int idx, float value);
  void insertBlock(int idx, FloatBuffer values);
  void insertIndexes(IntBuffer indexes, FloatBuffer values);
  void insertConstant(int idx, float value, int count);
}
