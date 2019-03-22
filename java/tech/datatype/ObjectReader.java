package tech.datatype;

import java.util.*;
import java.nio.*;

public interface ObjectReader extends Datatype
{
  Object read(int idx);
};
