(ns tech.datatype.argtypes
  (:require [tech.datatype.protocols :as dtype-proto]))


(defn arg->arg-type
  [arg]
  (cond
    (and (satisfies? dtype-proto/PToReader arg)
         (not (number? arg)))
    :reader
    (or (instance? Iterable arg)
        (satisfies? dtype-proto/PToIterable arg)) :iterable
    :else
    :scalar))
