package tech.datatype;

import clojure.lang.IFn;


public interface ObjectWriter extends IOBase, IFn
{
  void write(int idx, Object value);
};
