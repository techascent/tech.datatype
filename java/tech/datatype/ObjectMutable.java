package tech.datatype;
import java.nio.*;
import java.util.*;


public interface ObjectMutable extends MutableRemove
{
  void insert(int idx, Object value);
}
