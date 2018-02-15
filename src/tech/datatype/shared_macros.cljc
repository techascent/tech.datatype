(ns tech.datatype.shared-macros
  (:require [clojure.core.matrix :as m]))


(defmacro check-range
  [item offset elem-count]
  `(when-not (<= (+ ~offset ~elem-count)
                 (m/ecount ~item))
     (throw (ex-info "offset + n-elems > ecount range violation"
                     {:offset ~offset
                      :n-elems ~elem-count
                      :length (m/ecount ~item)}))))
