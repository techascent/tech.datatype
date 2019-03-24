package tech.datatype;

import it.unimi.dsi.fastutil.shorts.ShortIterator;


public interface ShortIter extends Datatype, ShortIterator
{
  short current();
}
