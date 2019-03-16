package tech.datatype;

import java.nio.*;
import it.unimi.dsi.fastutil.booleans.BooleanList;

public interface BooleanReader
{
  boolean read(int idx);
  void readBlock(int offset, BooleanList destination);
  void readIndexes(IntBuffer indexes, BooleanList destination);
};
