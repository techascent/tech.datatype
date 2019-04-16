package tech.datatype;

import clojure.lang.IFn;


public interface ObjectReader extends IOBase, Iterable, IFn
{
  Object read(int idx);
};
