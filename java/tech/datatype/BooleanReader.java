package tech.datatype;

import java.nio.*;
import it.unimi.dsi.fastutil.booleans.BooleanList;

public interface BooleanReader extends Datatype
{
  boolean read(int idx);
};
