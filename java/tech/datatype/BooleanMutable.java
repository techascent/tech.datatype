package tech.datatype;
import java.nio.*;
import it.unimi.dsi.fastutil.booleans.BooleanList;

public interface BooleanMutable extends MutableRemove
{
  void insert(int idx, boolean value);
}
