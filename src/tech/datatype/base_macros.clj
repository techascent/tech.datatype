(ns tech.datatype.base-macros
  (:require [clojure.core.matrix :as m]))

(defmacro try-catch-any
  [try-body & catch-body]
  `(try
     ~try-body
     (catch Throwable ~(first catch-body)
       ~@(rest catch-body))))


(defmacro check-range
  [item offset elem-count]
  `(when-not (<= (+ ~offset ~elem-count)
                 (m/ecount ~item))
     (throw (ex-info (format "%s offset + n-elems > ecount range violation"
                             ~(name item))
                     {:offset ~offset
                      :n-elems ~elem-count
                      :length (m/ecount ~item)}))))
