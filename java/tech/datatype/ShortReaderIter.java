package tech.datatype;

import clojure.lang.Keyword;


public class ShortReaderIter implements IOBase, ShortIter
{
  int idx;
  int num_elems;
  ShortReader reader;
  public ShortReaderIter(ShortReader _reader)
  {
    idx = 0;
    num_elems = _reader.size();
    reader = _reader;
  }
  public Keyword getDatatype() { return reader.getDatatype(); }
  public int size() { return num_elems - idx; }
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
