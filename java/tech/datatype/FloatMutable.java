package tech.datatype;
import java.nio.*;


public interface FloatMutable extends MutableRemove
{
  void insert(int idx, float value);
  void insertConstant(int idx, float value, int count);
  void insertBlock(int idx, FloatBuffer values);
}
