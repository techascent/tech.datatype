package tech.datatype;

import it.unimi.dsi.fastutil.booleans.BooleanIterator;


public interface BooleanIter extends Datatype, BooleanIterator
{
  boolean current();
}
