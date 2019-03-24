package tech.datatype;

import it.unimi.dsi.fastutil.bytes.ByteIterator;


public interface ByteIter extends Datatype, ByteIterator
{
  byte current();
}
