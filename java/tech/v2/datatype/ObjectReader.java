package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.Sequential;
import clojure.lang.RT;
import clojure.lang.ISeq;
import clojure.lang.Indexed;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;
import java.util.stream.Stream;


public interface ObjectReader extends IOBase, Iterable, IFn,
				      List, RandomAccess, Sequential,
				      Indexed
{
  Object read(long idx);
  default Object getDatatype () { return Keyword.intern(null, "object"); }
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
    return new ObjectReaderIter(this);
  }
  default Object invoke(Object arg) {
    return read(RT.uncheckedLongCast(arg));
  }
  default Object applyTo(ISeq items) {
    if (1 == items.count()) {
      return invoke(items.first());
    } else {
      //Abstract method error
      return invoke(items.first(), items.next());
    }
  }
  default Stream typedStream() {
    return stream();
  }
  default int count() { return size(); }
  default Object nth(int idx) { return read(idx); }
  default Object nth(int idx, Object notFound) {
    if (idx >= 0 && idx <= size()) {
      return read(idx);
    } else {
      return notFound;
    }
  }
}
