package tech.datatype;

import java.util.Iterator;


public interface ObjectIter extends Datatype, Iterator
{
  Object current();
}
