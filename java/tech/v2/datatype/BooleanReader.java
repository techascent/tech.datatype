package tech.v2.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import java.util.Iterator;

public interface BooleanReader extends IOBase, Iterable, IFn
{
  boolean read(long idx);
  default Keyword getDatatype () { return Keyword.intern(null, "boolean"); }
  default Iterator iterator() {
    return new BooleanReaderIter(this);
  }
  default Object invoke(Object arg) {
    return read((long)arg);
  }
};
