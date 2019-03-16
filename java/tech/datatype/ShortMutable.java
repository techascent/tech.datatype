package tech.datatype;
import java.nio.*;


public interface ShortMutable
{
  void insert(int idx, short value);
  void insertBlock(int idx, ShortBuffer values);
  void insertIndexes(IntBuffer indexes, ShortBuffer values);
  void insertConstant(int idx, short value, int count);
}
