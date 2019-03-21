package tech.datatype;

import java.util.*;
import java.nio.*;

public interface ObjectReader extends Datatype
{
  Object read(int idx);
  void readBlock(int offset, List destination);
  void readIndexes(IntBuffer indexes, List destination);
};
