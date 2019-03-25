package tech.datatype;

import clojure.lang.IFn;


public interface ByteWriter extends IOBase, IFn
{
  void write(int idx, byte value);
}
