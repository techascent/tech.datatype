package tech.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import java.util.Iterator;


public interface ShortReader extends IOBase, Iterable, IFn
{
  short read(long idx);
  default Keyword getDatatype () { return Keyword.intern(null, "int16"); }
  default Iterator iterator() {
    return new ShortReaderIter(this);
  }
  default Object invoke(Object arg) {
    return read((long)arg);
  }
}
