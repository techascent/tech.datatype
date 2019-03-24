package tech.datatype;

import it.unimi.dsi.fastutil.booleans.BooleanIterator;
import clojure.lang.Keyword;


public class BooleanReaderIter implements IOBase, BooleanIterator
{
  int idx;
  int num_elems;
  BooleanReader reader;
  public BooleanReaderIter(BooleanReader _reader)
  {
    idx = 0;
    num_elems = _reader.size();
    reader = _reader;
  }
  public Keyword getDatatype() { return reader.getDatatype(); }
  public int size() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public boolean nextBoolean() {
    boolean retval = reader.read(idx);
    ++idx;
    return retval;
  }
  public boolean current() {
    return reader.read(idx);
  }
}
