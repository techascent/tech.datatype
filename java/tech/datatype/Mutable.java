package tech.datatype;
import java.nio.*;
import java.util.*;


public interface Mutable
{
  void insert(int idx, Object value);
  void insertBlock(int idx, List values);
  void insertIndexes(IntBuffer indexes, List values);
  void insertConstant(int idx, Object value, int count);
  void remove(int idx);
  void removeRange(int idx, int count);
}
