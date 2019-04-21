package tech.v2.datatype;

import clojure.lang.Keyword;


public class FloatReaderIter implements IOBase, FloatIter
{
  long idx;
  long num_elems;
  FloatReader reader;
  public FloatReaderIter(FloatReader _reader)
  {
    idx = 0;
    num_elems = _reader.lsize();
    reader = _reader;
  }
  public Keyword getDatatype() { return reader.getDatatype(); }
  public long lsize() { return num_elems - idx; }
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
