(ns tech.v2.datatype.base-macros
  (:require [tech.v2.datatype.protocols :as dtype-proto]))

(defmacro try-catch-any
  [try-body & catch-body]
  `(try
     ~try-body
     (catch Throwable ~(first catch-body)
       ~@(rest catch-body))))


(defmacro check-range
  [item offset elem-count]
  `(when-not (<= (+ ~offset ~elem-count)
                 (dtype-proto/ecount ~item))
     (throw (ex-info (format "%s offset + n-elems > ecount range violation"
                             ~(name item))
                     {:offset ~offset
                      :n-elems ~elem-count
                      :length (dtype-proto/ecount ~item)}))))
