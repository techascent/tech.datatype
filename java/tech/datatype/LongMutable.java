package tech.datatype;
import java.nio.*;


public interface LongMutable extends MutableRemove
{
  void insert(int idx, long value);
  void insertConstant(int idx, long value, int count);
  void insertBlock(int idx, LongBuffer values);
  void insertIndexes(IntBuffer indexes, LongBuffer values);
}
