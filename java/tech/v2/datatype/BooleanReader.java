package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;
import clojure.lang.RT;


public interface BooleanReader extends IOBase, Iterable, IFn, List, RandomAccess
{
  boolean read(long idx);
  default Object getDatatype () { return Keyword.intern(null, "boolean"); }
  default int size() { return RT.intCast(lsize()); }
  default Object get(int idx) { return read(idx); }
  default boolean isEmpty() { return lsize() == 0; }
  default Object[] toArray() {
    int nElems = size();
    Object[] data = new Object[nElems];

    for(int idx=0; idx < nElems; ++idx) {
      data[idx] = read(idx);
    }
    return data;
  }
  default Iterator iterator() {
    return new BooleanReaderIter(this);
  }
  default Object invoke(Object arg) {
    return read(RT.longCast(arg));
  }
};
