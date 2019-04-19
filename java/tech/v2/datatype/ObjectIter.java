package tech.v2.datatype;

import java.util.Iterator;


public interface ObjectIter extends Datatype, Iterator
{
  Object current();
}
