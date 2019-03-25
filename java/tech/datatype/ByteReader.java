package tech.datatype;

import clojure.lang.IFn;


public interface ByteReader extends IOBase, Iterable, IFn
{
  byte read(int idx);
}
