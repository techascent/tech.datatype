(ns tech.v2.datatype.primitive
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.array :as dtype-ary])
  (:import [tech.v2.datatype ObjectReader]))



(defmacro implement-scalar-primitive
  [cls datatype]
  `(clojure.core/extend
       ~cls
     dtype-proto/PDatatype
     {:get-datatype (fn [item#] ~datatype)}
     dtype-proto/PCountable
     {:ecount (fn [item#] 1)}
     dtype-proto/PToReader
     {:->reader (fn [item# options#]
                  (-> (dtype-ary/make-array-of-type ~datatype [item#] true)
                      (dtype-proto/->reader options#)))}))


(implement-scalar-primitive Byte :int8)
(implement-scalar-primitive Short :int16)
(implement-scalar-primitive Integer :int32)
(implement-scalar-primitive Long :int64)
(implement-scalar-primitive Float :float32)
(implement-scalar-primitive Double :float64)
(implement-scalar-primitive Boolean :boolean)
