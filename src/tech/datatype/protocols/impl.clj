(ns tech.datatype.protocols.impl
 (:require [tech.datatype.protocols :as dtype-proto])
 (:import [tech.datatype.protocols PDatatype]
          [tech.datatype Datatype]))


(defn safe-get-datatype
  "Fastest version of a get-datatype that assumes item implements PDatatype
  but does not require it."
  [item]
  (cond
    (instance? PDatatype item) (.get_datatype ^PDatatype item)
    (instance? Datatype item) (.getDatatype ^Datatype item)
    (satisfies? dtype-proto/PDatatype item)
    (dtype-proto/get-datatype item)
    :else
    :object))
