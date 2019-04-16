package tech.datatype;

import clojure.lang.IFn;


public interface FloatWriter extends IOBase, IFn
{
  void write(int idx, float value);
};
