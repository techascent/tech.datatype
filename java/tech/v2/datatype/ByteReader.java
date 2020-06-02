package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.Sequential;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;
import clojure.lang.RT;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public interface ByteReader extends IOBase, Iterable, IFn,
				    List, RandomAccess, Sequential
{
  byte read(long idx);
  default int size() { return RT.intCast(lsize()); }
  default Object get(int idx) { return read(idx); }
  default boolean isEmpty() { return lsize() == 0; }
  default Object getDatatype () { return Keyword.intern(null, "int8"); }
  default Object[] toArray() {
    int nElems = size();
    Object[] data = new Object[nElems];

    for(int idx=0; idx < nElems; ++idx) {
      data[idx] = read(idx);
    }
    return data;
  }
  default Iterator iterator() {
    return new ByteReaderIter(this);
  }
  default Object invoke(Object arg) {
    return read( RT.longCast( arg ));
  }
  default IntStream typedStream() {
    return IntStream.range(0, size()).map(i -> read(i));
  }
}
