package tech.datatype;

import clojure.lang.IFn;


public interface DoubleWriter extends IOBase, IFn
{
  void write(int idx, double value);
}
