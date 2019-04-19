(ns tech.datatype.argtypes
  (:require [tech.datatype.protocols :as dtype-proto])
  (:import [tech.datatype.protocols PToReader]
           [java.util RandomAccess]))


(defn arg->arg-type
  [arg]
  (cond
    (number? arg)
    :scalar
    (or (instance? tech.datatype.protocols.PToReader arg)
        (instance? RandomAccess arg)
        (satisfies? dtype-proto/PToReader arg))
    :reader
    (or (instance? Iterable arg)
        (satisfies? dtype-proto/PToIterable arg))
    :iterable
    :else
    :scalar))
