package tech.v2.datatype;

import it.unimi.dsi.fastutil.bytes.ByteIterator;


public interface ByteIter extends Datatype, ByteIterator
{
  byte current();
}
