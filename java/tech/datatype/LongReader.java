package tech.datatype;

import clojure.lang.IFn;


public interface LongReader extends IOBase, Iterable, IFn
{
  long read(int idx);
}
