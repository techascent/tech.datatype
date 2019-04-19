package tech.v2.datatype;

import clojure.lang.Keyword;


public class IntReaderIter implements IOBase, IntIter
{
  long idx;
  long num_elems;
  IntReader reader;
  public IntReaderIter(IntReader _reader)
  {
    idx = 0;
    num_elems = _reader.size();
    reader = _reader;
  }
  public Keyword getDatatype() { return reader.getDatatype(); }
  public long size() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public int nextInt() {
    int retval = reader.read(idx);
    ++idx;
    return retval;
  }
  public int current() {
    return reader.read(idx);
  }
}
