package tech.datatype;

import java.nio.*;

public interface FloatWriter extends Datatype
{
  void write(int idx, float value);
};
