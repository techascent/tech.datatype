(ns tech.tensor.index-system
  (:require [tech.datatype :as dtype]
            [tech.datatype.base :as dtype-base]
            [tech.tensor.dimensions :as dims]
            [tech.tensor.dimensions.shape :as shape]
            [tech.tensor.utils :as utils]
            [tech.datatype.typecast :as typecast]
            [tech.datatype.argsort :as argsort]
            [tech.datatype.reader :as reader]
            [tech.datatype.unary-op :as unary-op]
            [tech.datatype.binary-op :as binary-op]
            [tech.datatype.reduce-op :as reduce-op]
            [tech.datatype.boolean-op :as boolean-op]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.binary-search :as dtype-search])
  (:import [tech.datatype
            IndexingSystem$Forward
            IndexingSystem$Backward
            IntReader]
           [clojure.lang IDeref]
           [tech.tensor.dimensions Dimensions]))

(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)
