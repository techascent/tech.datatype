package tech.datatype;
import java.nio.*;


public interface DoubleMutable
{
  void insert(int idx, double value);
  void insertBlock(int idx, DoubleBuffer values);
  void insertIndexes(IntBuffer indexes, DoubleBuffer values);
  void insertConstant(int idx, double value, int count);
}
