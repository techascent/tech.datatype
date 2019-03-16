package tech.datatype;
import java.nio.*;


public interface IntMutable
{
  void insert(int idx, int value);
  void insertBlock(int idx, IntBuffer values);
  void insertIndexes(IntBuffer indexes, IntBuffer values);
  void insertConstant(int idx, int value, int count);
}
