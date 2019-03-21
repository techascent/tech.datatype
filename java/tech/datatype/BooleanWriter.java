package tech.datatype;

import java.nio.*;
import it.unimi.dsi.fastutil.booleans.BooleanList;

public interface BooleanWriter extends Datatype
{
  void write(int idx, boolean value);
  void writeConstant(int idx, boolean value, int count);
  void writeBlock(int offset, BooleanList values);
  void writeIndexes(IntBuffer indexes, BooleanList values);
}
