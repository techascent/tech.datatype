package tech.datatype;

import java.nio.*;

public interface ByteWriter extends IOBase
{
  void write(int idx, byte value);
}
