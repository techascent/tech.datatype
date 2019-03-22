package tech.datatype;
import java.nio.*;


public interface ByteMutable extends MutableRemove
{
  void insert(int idx, byte value);
  void insertConstant(int idx, byte value, int count);
}
