package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;


public interface ObjectReader extends IOBase, Iterable, IFn, List, RandomAccess
{
  Object read(long idx);
  default Keyword getDatatype () { return Keyword.intern(null, "object"); }
  default int size() { return (int) lsize(); }
  default Object get(int idx) { return read(idx); }
  default boolean isEmpty() { return lsize() == 0; }
  default Iterator iterator() {
    return new ObjectReaderIter(this);
  }
  default Object invoke(Object arg) {
    return read((long)arg);
  }
};
