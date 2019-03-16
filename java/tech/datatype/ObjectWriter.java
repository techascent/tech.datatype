package tech.datatype;

import java.util.*;
import java.nio.*;

public interface ObjectWriter
{
  void write(int idx, Object value);
  void writeBlock(int offset, List values);
  void writeIndexes(IntBuffer indexes, List values);
};
