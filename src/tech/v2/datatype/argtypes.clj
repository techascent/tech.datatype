(ns tech.v2.datatype.argtypes
  (:require [tech.v2.datatype.protocols :as dtype-proto])
  (:import [tech.v2.datatype.protocols PToReader]
           [java.util RandomAccess]))


(defn arg->arg-type
  [arg]
  (cond
    (number? arg)
    :scalar
    (or (instance? tech.v2.datatype.protocols.PToReader arg)
        (instance? RandomAccess arg)
        (satisfies? dtype-proto/PToReader arg))
    :reader
    (or (instance? Iterable arg)
        (satisfies? dtype-proto/PToIterable arg))
    :iterable
    :else
    :scalar))
