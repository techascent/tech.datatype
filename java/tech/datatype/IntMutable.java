package tech.datatype;
import java.nio.*;


public interface IntMutable extends MutableRemove
{
  void insert(int idx, int value);
  void insertConstant(int idx, int value, int count);
  void insertBlock(int idx, IntBuffer values);
  void insertIndexes(IntBuffer indexes, IntBuffer values);
}
