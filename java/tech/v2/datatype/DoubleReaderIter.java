package tech.v2.datatype;

import clojure.lang.Keyword;


public class DoubleReaderIter implements IOBase, DoubleIter
{
  long idx;
  long num_elems;
  DoubleReader reader;
  public DoubleReaderIter(DoubleReader _reader)
  {
    idx = 0;
    num_elems = _reader.lsize();
    reader = _reader;
  }
  public Keyword getDatatype() { return reader.getDatatype(); }
  public long lsize() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public double nextDouble() {
    double retval = reader.read(idx);
    ++idx;
    return retval;
  }
  public double current() {
    return reader.read(idx);
  }
}
