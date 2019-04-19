package tech.v2.datatype;

import it.unimi.dsi.fastutil.doubles.DoubleIterator;


public interface DoubleIter extends Datatype, DoubleIterator
{
  double current();
}
