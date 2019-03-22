package tech.datatype;

import java.nio.*;

public interface ByteWriter extends Datatype
{
  void write(int idx, byte value);
}
