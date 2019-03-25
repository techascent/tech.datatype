package tech.datatype;

import clojure.lang.IFn;


public interface BooleanWriter extends IOBase, IFn
{
  void write(int idx, boolean value);
}
