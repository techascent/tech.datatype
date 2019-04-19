package tech.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import java.util.Iterator;


public interface ByteReader extends IOBase, Iterable, IFn
{
  byte read(long idx);
  default Keyword getDatatype () { return Keyword.intern(null, "int8"); }
  default Iterator iterator() {
    return new ByteReaderIter(this);
  }
  default Object invoke(Object arg) {
    return read((long)arg);
  }
}
