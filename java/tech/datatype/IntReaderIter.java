package tech.datatype;

import it.unimi.dsi.fastutil.ints.IntIterator;
import clojure.lang.Keyword;


public class IntReaderIter implements IOBase, IntIterator
{
  int idx;
  int num_elems;
  IntReader reader;
  public IntReaderIter(IntReader _reader)
  {
    idx = 0;
    num_elems = _reader.size();
    reader = _reader;
  }
  public Keyword getDatatype() { return reader.getDatatype(); }
  public int size() { return num_elems - idx; }
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
