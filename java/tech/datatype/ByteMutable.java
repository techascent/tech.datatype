package tech.datatype;
import java.nio.*;


public interface ByteMutable
{
  void insert(int idx, byte value);
  void insertBlock(int idx, ByteBuffer values);
  void insertIndexes(IntBuffer indexes, ByteBuffer values);
  void insertConstant(int idx, byte value, int count);
}
