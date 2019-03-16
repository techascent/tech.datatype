package tech.datatype;
import java.nio.*;


public interface LongMutable
{
  void insert(int idx, long value);
  void insertBlock(int idx, LongBuffer values);
  void insertIndexes(IntBuffer indexes, LongBuffer values);
  void insertConstant(int idx, long value, int count);
}
