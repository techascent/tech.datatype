package tech.v2.datatype;


import java.util.Iterator;
import clojure.lang.Keyword;


public class IteratorObjectIter implements ObjectIter
{
  Iterator iter;
  boolean has_next;
  Object current_object;
  Keyword dtype;

  public IteratorObjectIter( Iterator _iter, Keyword _dtype )
  {
    iter = _iter;
    dtype = _dtype;
    has_next = iter.hasNext();
    if( has_next )
      current_object = iter.next();
  }
  public Keyword getDatatype() { return dtype; }
  public boolean hasNext() { return has_next; }
  public Object next() {
    //Force exception from base iterator.
    if (!has_next)
      iter.next();
    
    Object retval = current_object;
    has_next = iter.hasNext();
    if (has_next) {      
      current_object = iter.next();
    }
    else {
      current_object = null;
    }
    return retval;
  }
  public Object current() { return current_object; }
}
