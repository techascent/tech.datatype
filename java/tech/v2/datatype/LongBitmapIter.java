package tech.v2.datatype;


import clojure.lang.Keyword;
import org.roaringbitmap.IntIterator;


public class LongBitmapIter implements LongIter
{
  IntIterator iter;
  long currentValue;
  public LongBitmapIter(IntIterator _iter)
  {
    iter = _iter;
  }
  public Keyword getDatatype() { return Keyword.intern(null, "uint32"); }
  public boolean hasNext() { return iter.hasNext(); }
  public long nextLong() {
    currentValue = Integer.toUnsignedLong( iter.next() );
    return currentValue;
  }
  public long current() {
    return currentValue;
  }
};
