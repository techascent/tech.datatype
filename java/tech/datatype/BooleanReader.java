package tech.datatype;

import clojure.lang.IFn;


public interface BooleanReader extends IOBase, Iterable, IFn
{
  boolean read(int idx);
};
