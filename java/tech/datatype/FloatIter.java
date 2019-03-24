package tech.datatype;

import it.unimi.dsi.fastutil.floats.FloatIterator;


public interface FloatIter extends Datatype, FloatIterator
{
  float current();
}
