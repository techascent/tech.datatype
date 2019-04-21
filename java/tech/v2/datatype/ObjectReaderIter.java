package tech.v2.datatype;

import clojure.lang.Keyword;


public class ObjectReaderIter implements IOBase, ObjectIter
{
  long idx;
  long num_elems;
  ObjectReader reader;
  public ObjectReaderIter(ObjectReader _reader)
  {
    idx = 0;
    num_elems = _reader.lsize();
    reader = _reader;
  }
  public Keyword getDatatype() { return reader.getDatatype(); }
  public long lsize() { return num_elems - idx; }
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
