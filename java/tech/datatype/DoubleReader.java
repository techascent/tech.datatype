package tech.datatype;

import clojure.lang.IFn;


public interface DoubleReader extends IOBase, Iterable, IFn
{
  double read(int idx);
}
