package tech.datatype;

import clojure.lang.IFn;


public interface IntWriter extends IOBase, IFn
{
  void write(int idx, int value);
}
