package tech.datatype;

import java.nio.*;

public interface DoubleWriter extends Datatype
{
  void write(int idx, double value);
}
