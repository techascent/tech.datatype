package tech.v2.datatype;

import clojure.lang.Keyword;


public class ShortReaderIter implements IOBase, ShortIter
{
  long idx;
  long num_elems;
  ShortReader reader;
  public ShortReaderIter(ShortReader _reader)
  {
    idx = 0;
    num_elems = _reader.lsize();
    reader = _reader;
  }
  public Object getDatatype() { return reader.getDatatype(); }
  public long lsize() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public short nextShort() {
    short retval = reader.read(idx);
    ++idx;
    return retval;
  }
  public short current() {
    return reader.read(idx);
  }
}
