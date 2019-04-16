package tech.datatype;

import it.unimi.dsi.fastutil.longs.LongIterator;


public interface LongIter extends Datatype, LongIterator
{
  long current();
}
