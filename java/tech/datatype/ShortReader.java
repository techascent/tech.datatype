package tech.datatype;

import clojure.lang.IFn;


public interface ShortReader extends IOBase, Iterable, IFn
{
  short read(int idx);
}
