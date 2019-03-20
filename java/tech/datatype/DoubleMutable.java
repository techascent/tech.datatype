package tech.datatype;
import java.nio.*;


public interface DoubleMutable extends MutableRemove
{
  void insert(int idx, double value);
  void insertConstant(int idx, double value, int count);
  void insertBlock(int idx, DoubleBuffer values);
  void insertIndexes(IntBuffer indexes, DoubleBuffer values);
}
