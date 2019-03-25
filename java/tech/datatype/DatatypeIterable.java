package tech.datatype;


import clojure.lang.Keyword;


public interface DatatypeIterable extends Datatype, Iterable
{
  Object iteratorOfType(Keyword dtype, boolean unchecked);
};
