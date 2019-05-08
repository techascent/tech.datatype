(ns tech.v2.datatype.argtypes
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.shape :as dtype-shape])
  (:import [tech.v2.datatype.protocols PToReader]
           [java.util RandomAccess]))


(defn arg->arg-type
  [arg]
  (cond
    (dtype-shape/scalar? arg)
    :scalar
    (or (instance? tech.v2.datatype.protocols.PToReader arg)
        (instance? RandomAccess arg)
        (dtype-proto/convertible-to-reader? arg))
    :reader
    (or (instance? Iterable arg)
        (dtype-proto/convertible-to-iterable? arg))
    :iterable
    :else
    :scalar))
