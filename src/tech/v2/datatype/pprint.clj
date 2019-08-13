(ns tech.v2.datatype.pprint)


;; pretty-printing utilities for matrices
(def ^:dynamic *number-format* "%.3f")


(defn format-num [x]
  (if (integer? x)
    (str x)
    (format *number-format* (double x))))


(defn format-object
  [x]
  (if (number? x)
    (format-num x)
    (str x)))
