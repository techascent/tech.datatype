package tech.datatype;

import it.unimi.dsi.fastutil.floats.FloatIterator;
import clojure.lang.Keyword;


public class FloatReaderIter implements IOBase, FloatIterator
{
  int idx;
  int num_elems;
  FloatReader reader;
  public FloatReaderIter(FloatReader _reader)
  {
    idx = 0;
    num_elems = _reader.size();
    reader = _reader;
  }
  public Keyword getDatatype() { return reader.getDatatype(); }
  public int size() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public float nextFloat() {
    float retval = reader.read(idx);
    ++idx;
    return retval;
  }
  public float current() {
    return reader.read(idx);
  }
}
