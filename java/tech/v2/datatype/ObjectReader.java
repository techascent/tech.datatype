package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import java.util.Iterator;


public interface ObjectReader extends IOBase, Iterable, IFn
{
  Object read(long idx);
  default Keyword getDatatype () { return Keyword.intern(null, "object"); }
  default Iterator iterator() {
    return new ObjectReaderIter(this);
  }
  default Object invoke(Object arg) {
    return read((long)arg);
  }
};
