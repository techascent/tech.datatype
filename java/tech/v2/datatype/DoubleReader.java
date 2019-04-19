package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import java.util.Iterator;


public interface DoubleReader extends IOBase, Iterable, IFn
{
  double read(long idx);
  default Keyword getDatatype () { return Keyword.intern(null, "float64"); }
  default Iterator iterator() {
    return new DoubleReaderIter(this);
  }
  default Object invoke(Object arg) {
    return read((long)arg);
  }
}
