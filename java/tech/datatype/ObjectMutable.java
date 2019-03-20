package tech.datatype;
import java.nio.*;
import java.util.*;


public interface ObjectMutable extends MutableRemove
{
  void insert(int idx, Object value);
  void insertBlock(int idx, List values);
  void insertConstant(int idx, Object value, int count);
  void insertIndexes(IntBuffer indexes, List values);
}
