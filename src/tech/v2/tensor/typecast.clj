(ns tech.v2.tensor.typecast
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.tensor.protocols :as tensor-proto])
  (:import [tech.v2.tensor
            ByteTensorReader ShortTensorReader IntTensorReader
            LongTensorReader FloatTensorReader DoubleTensorReader
            BooleanTensorReader ObjectTensorReader]))



(defn datatype->tensor-reader-type
  [datatype]
  (case (casting/safe-flatten datatype)
    :int8 'tech.v2.tensor.ByteTensorReader
    :int16 'tech.v2.tensor.ShortTensorReader
    :int32 'tech.v2.tensor.IntTensorReader
    :int64 'tech.v2.tensor.LongTensorReader
    :float32 'tech.v2.tensor.FloatTensorReader
    :float64 'tech.v2.tensor.DoubleTensorReader
    :boolean 'tech.v2.tensor.BooleanTensorReader
    :object 'tech.v2.tensor.ObjectTensorReader))



(defmacro implement-reader-cast
  [datatype]
  `(if (instance? ~(resolve (datatype->tensor-reader-type datatype)) ~'item)
     ~'item
     (tensor-proto/->tensor-reader ~'item {:datatype ~datatype
                                           :unchecked? ~'unchecked?})))



(defn ->int8-reader ^ByteTensorReader [item unchecked?] (implement-reader-cast :int8))
(defn ->int16-reader ^ShortTensorReader [item unchecked?] (implement-reader-cast :int16))
(defn ->int32-reader ^IntTensorReader [item unchecked?] (implement-reader-cast :int32))
(defn ->int64-reader ^LongTensorReader [item unchecked?] (implement-reader-cast :int64))
(defn ->float32-reader ^FloatTensorReader [item unchecked?] (implement-reader-cast :float32))
(defn ->float64-reader ^DoubleTensorReader [item unchecked?] (implement-reader-cast :float64))
(defn ->boolean-reader ^BooleanTensorReader [item unchecked?] (implement-reader-cast :boolean))
(defn ->object-reader ^ObjectTensorReader [item unchecked?] (implement-reader-cast :object))


(defmacro datatype->tensor-reader
  "Convert an item into a know tensor reader type."
  [item datatype & [unchecked?]]
  (case (casting/safe-flatten datatype)
    :int8 `(->int8-reader ~item ~unchecked?)
    :int16 `(->int16-reader ~item ~unchecked?)
    :int32 `(->int32-reader ~item ~unchecked?)
    :int64 `(->int64-reader ~item ~unchecked?)
    :float32 `(->float32-reader ~item ~unchecked?)
    :float64 `(->float64-reader ~item ~unchecked?)
    :boolean `(->boolean-reader ~item ~unchecked?)
    :object `(->object-reader ~item ~unchecked?)))
