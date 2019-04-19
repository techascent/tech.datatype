package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import java.util.Iterator;

public interface LongReader extends IOBase, Iterable, IFn
{
  long read(long idx);
  default Keyword getDatatype () { return Keyword.intern(null, "int64"); }
  default Iterator iterator() {
    return new LongReaderIter(this);
  }
  default Object invoke(Object arg) {
    return read((long)arg);
  }
}
