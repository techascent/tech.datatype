package tech.datatype;

import java.nio.*;
import it.unimi.dsi.fastutil.booleans.BooleanList;

public interface BooleanWriter extends Datatype
{
  void write(int idx, boolean value);
}
