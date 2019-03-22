(ns tech.datatype.primitive
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.array :as dtype-ary]
            [clojure.core.matrix.protocols :as mp]
            [tech.datatype.reader :as reader]
            [clojure.core.matrix.macros :refer [c-for]])
  (:import [tech.datatype ObjectReader]))



(defmacro implement-scalar-primitive
  [cls datatype]
  `(clojure.core/extend
       ~cls
     dtype-proto/PDatatype
     {:get-datatype (fn [item#] ~datatype)}
     dtype-proto/PToReader
     {:->reader-of-type (fn [item# reader-dtype# unsigned?#]
                          (-> (dtype-ary/make-array-of-type ~datatype [item#] true)
                              (dtype-proto/->reader-of-type reader-dtype# unsigned?#)))}
     mp/PElementCount
     {:element-count (fn [item#] 1)}))


(implement-scalar-primitive Byte :int8)
(implement-scalar-primitive Short :int16)
(implement-scalar-primitive Integer :int32)
(implement-scalar-primitive Long :int64)
(implement-scalar-primitive Float :float32)
(implement-scalar-primitive Double :float64)
(implement-scalar-primitive Boolean :boolean)
