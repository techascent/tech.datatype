package tech.datatype;

import clojure.lang.IFn;


public interface LongWriter extends IOBase, IFn
{
  void write(int idx, long value);
};
