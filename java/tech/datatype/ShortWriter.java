package tech.datatype;

import java.nio.*;

public interface ShortWriter extends IOBase
{
  void write(int idx, short value);
}
