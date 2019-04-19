package tech.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import java.util.Iterator;


public interface IntReader extends IOBase, Iterable, IFn
{
  int read(long idx);
  default Keyword getDatatype () { return Keyword.intern(null, "int32"); }
  default Iterator iterator() {
    return new IntReaderIter(this);
  }
  default Object invoke(Object arg) {
    return read((long)arg);
  }
}
