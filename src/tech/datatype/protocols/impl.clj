(ns tech.datatype.protocols.impl
 (:require [tech.datatype.protocols :as dtype-proto])
 (:import [tech.datatype.protocols PDatatype]
          [tech.datatype Datatype]))


(defn safe-get-datatype
  "Fastest version of a get-datatype that assumes item implements PDatatype
  but does not require it."
  [item]
  (dtype-proto/get-datatype item))
