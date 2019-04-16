(ns tech.tensor.typecast
  (:require [tech.datatype.casting :as casting]
            [tech.tensor.protocols :as tensor-proto])
  (:import [tech.tensor
            ByteReader ShortReader IntReader LongReader
            FloatReader DoubleReader BooleanReader ObjectReader]))



(defn datatype->tensor-reader-type
  [datatype]
  (case (casting/safe-flatten datatype)
    :int8 'tech.tensor.ByteReader
    :int16 'tech.tensor.ShortReader
    :int32 'tech.tensor.IntReader
    :int64 'tech.tensor.LongReader
    :float32 'tech.tensor.FloatReader
    :float64 'tech.tensor.DoubleReader
    :boolean 'tech.tensor.BooleanReader
    :object 'tech.tensor.ObjectReader))



(defmacro implement-reader-cast
  [datatype]
  `(if (instance? ~(resolve (datatype->tensor-reader-type datatype)) ~'item)
     ~'item
     (tensor-proto/->tensor-reader-of-type ~'item ~datatype ~'unchecked?)))



(defn ->int8-reader ^ByteReader [item unchecked?] (implement-reader-cast :int8))
(defn ->int16-reader ^ShortReader [item unchecked?] (implement-reader-cast :int16))
(defn ->int32-reader ^IntReader [item unchecked?] (implement-reader-cast :int32))
(defn ->int64-reader ^LongReader [item unchecked?] (implement-reader-cast :int64))
(defn ->float32-reader ^FloatReader [item unchecked?] (implement-reader-cast :float32))
(defn ->float64-reader ^DoubleReader [item unchecked?] (implement-reader-cast :float64))
(defn ->boolean-reader ^BooleanReader [item unchecked?] (implement-reader-cast :boolean))
(defn ->object-reader ^ObjectReader [item unchecked?] (implement-reader-cast :object))


(defmacro datatype->reader
  [datatype reader & [unchecked?]]
  (case (casting/safe-flatten datatype)
    :int8 `(->int8-reader ~reader ~unchecked?)
    :int16 `(->int16-reader ~reader ~unchecked?)
    :int32 `(->int32-reader ~reader ~unchecked?)
    :int64 `(->int64-reader ~reader ~unchecked?)
    :float32 `(->float32-reader ~reader ~unchecked?)
    :float64 `(->float64-reader ~reader ~unchecked?)
    :boolean `(->boolean-reader ~reader ~unchecked?)
    :object `(->object-reader ~reader ~unchecked?)))
