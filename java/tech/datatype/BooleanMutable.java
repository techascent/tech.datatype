package tech.datatype;
import java.nio.*;
import it.unimi.dsi.fastutil.booleans.BooleanList;

public interface BooleanMutable extends MutableRemove
{
  void insert(int idx, boolean value);
  void insertConstant(int idx, boolean value, int count);
  void insertBlock(int idx, BooleanList values);
  void insertIndexes(IntBuffer indexes, BooleanList values);
}
