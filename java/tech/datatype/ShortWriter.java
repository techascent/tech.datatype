package tech.datatype;

import clojure.lang.IFn;


public interface ShortWriter extends IOBase, IFn
{
  void write(int idx, short value);
}
