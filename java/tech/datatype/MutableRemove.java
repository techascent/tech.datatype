package tech.datatype;

import java.nio.IntBuffer;

public interface MutableRemove
{
  void remove(int idx);
  void removeRange(int idx, int count);
  void removeIndexes(IntBuffer indexes);
}
