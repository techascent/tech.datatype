package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import java.util.Iterator;

public interface FloatReader extends IOBase, Iterable, IFn
{
  float read(long idx);
  default Keyword getDatatype () { return Keyword.intern(null, "float32"); }
  default Iterator iterator() {
    return new FloatReaderIter(this);
  }
  default Object invoke(Object arg) {
    return read((long)arg);
  }
}
