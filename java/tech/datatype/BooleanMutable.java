package tech.datatype;
import java.nio.*;
import it.unimi.dsi.fastutil.booleans.BooleanList;

public interface BooleanMutable
{
  void insert(int idx, boolean value);
  void insertBlock(int idx, BooleanList values);
  void insertIndexes(IntBuffer indexes, BooleanList values);
  void insertConstant(int idx, boolean value, int count);
}
