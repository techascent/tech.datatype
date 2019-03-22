package tech.datatype;

import java.nio.*;

public interface LongWriter extends Datatype
{
  void write(int idx, long value);
};
