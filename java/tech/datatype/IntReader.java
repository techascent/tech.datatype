package tech.datatype;

import clojure.lang.IFn;


public interface IntReader extends IOBase, Iterable, IFn
{
  int read(int idx);
}
