package tech.datatype;
import java.nio.*;


public interface ShortMutable extends MutableRemove
{
  void insert(int idx, short value);
  void insertConstant(int idx, short value, int count);
  void insertBlock(int idx, ShortBuffer values);
}
