package tech.datatype;

import it.unimi.dsi.fastutil.bytes.ByteIterator;
import clojure.lang.Keyword;


public class ByteReaderIter implements IOBase, ByteIterator
{
  int idx;
  int num_elems;
  ByteReader reader;
  public ByteReaderIter(ByteReader _reader)
  {
    idx = 0;
    num_elems = _reader.size();
    reader = _reader;
  }
  public Keyword getDatatype() { return reader.getDatatype(); }
  public int size() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public byte nextByte() {
    byte retval = reader.read(idx);
    ++idx;
    return retval;
  }
  public byte current() {
    return reader.read(idx);
  }
}
