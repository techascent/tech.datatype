(ns tech.tensor.typecast
  (:require [tech.datatype.casting :as casting]
            [tech.tensor.protocols :as tensor-proto])
  (:import [tech.tensor
            ByteTensorReader ShortTensorReader IntTensorReader
            LongTensorReader FloatTensorReader DoubleTensorReader
            BooleanTensorReader ObjectTensorReader]))



(defn datatype->tensor-reader-type
  [datatype]
  (case (casting/safe-flatten datatype)
    :int8 'tech.tensor.ByteTensorReader
    :int16 'tech.tensor.ShortTensorReader
    :int32 'tech.tensor.IntTensorReader
    :int64 'tech.tensor.LongTensorReader
    :float32 'tech.tensor.FloatTensorReader
    :float64 'tech.tensor.DoubleTensorReader
    :boolean 'tech.tensor.BooleanTensorReader
    :object 'tech.tensor.ObjectTensorReader))



(defmacro implement-reader-cast
  [datatype]
  `(if (instance? ~(resolve (datatype->tensor-reader-type datatype)) ~'item)
     ~'item
     (tensor-proto/->tensor-reader-of-type ~'item ~datatype ~'unchecked?)))



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
