(ns tech.v2.datatype.protocols.impl
 (:require [tech.v2.datatype.protocols :as dtype-proto])
 (:import [tech.v2.datatype.protocols PDatatype]
          [tech.v2.datatype Datatype]))


(defn safe-get-datatype
  "Fastest version of a get-datatype that assumes item implements PDatatype
  but does not require it."
  [item]
  (dtype-proto/get-datatype item))
