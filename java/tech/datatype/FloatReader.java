package tech.datatype;

import clojure.lang.IFn;


public interface FloatReader extends IOBase, Iterable, IFn
{
  float read(int idx);
}
