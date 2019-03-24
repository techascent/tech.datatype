package tech.datatype;

import clojure.lang.Keyword;
import java.util.Iterator;

public class ObjectReaderIter implements IOBase, Iterator
{
  int idx;
  int num_elems;
  ObjectReader reader;
  public ObjectReaderIter(ObjectReader _reader)
  {
    idx = 0;
    num_elems = _reader.size();
    reader = _reader;
  }
  public Keyword getDatatype() { return reader.getDatatype(); }
  public int size() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public Object next() {
    Object retval = reader.read(idx);
    ++idx;
    return retval;
  }
  public Object current() {
    return reader.read(idx);
  }
}
