package tech.v2.datatype;

import clojure.lang.Keyword;


public class LongReaderIter implements IOBase, LongIter
{
  long idx;
  long num_elems;
  LongReader reader;
  public LongReaderIter(LongReader _reader)
  {
    idx = 0;
    num_elems = _reader.size();
    reader = _reader;
  }
  public Keyword getDatatype() { return reader.getDatatype(); }
  public long size() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public long nextLong() {
    long retval = reader.read(idx);
    ++idx;
    return retval;
  }
  public long current() {
    return reader.read(idx);
  }
}
