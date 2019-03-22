package tech.datatype;

import java.util.*;
import java.nio.*;

public interface ObjectWriter extends Datatype
{
  void write(int idx, Object value);
};
