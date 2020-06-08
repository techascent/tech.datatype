package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.Sequential;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;
import java.util.stream.IntStream;
import clojure.lang.RT;
import clojure.lang.ISeq;


public interface ShortReader extends IOBase, Iterable, IFn,
				     List, RandomAccess, Sequential
{
  short read(long idx);
  default Object getDatatype () { return Keyword.intern(null, "int16"); }
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
    return new ShortReaderIter(this);
  }
  default Object invoke(Object arg) {
    return read(RT.longCast(arg));
  }
  default Object applyTo(ISeq items) {
    if (1 == items.count()) {
      return invoke(items.first());
    } else {
      //Abstract method error
      return invoke(items.first(), items.next());
    }
  }
  default IntStream typedStream() {
    return IntStream.range(0, size()).map(i -> read(i));
  }
}
