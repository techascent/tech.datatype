(ns tech.v2.tensor.typecast
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.tensor.protocols :as tensor-proto])
  (:import [tech.v2.tensor
            ByteTensorReader ShortTensorReader IntTensorReader
            LongTensorReader FloatTensorReader DoubleTensorReader
            BooleanTensorReader ObjectTensorReader
            ByteTensorWriter ShortTensorWriter IntTensorWriter
            LongTensorWriter FloatTensorWriter DoubleTensorWriter
            BooleanTensorWriter ObjectTensorWriter
            ]))



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


(defn datatype->tensor-writer-type
  [datatype]
  (case (casting/safe-flatten datatype)
    :int8 'tech.v2.tensor.ByteTensorWriter
    :int16 'tech.v2.tensor.ShortTensorWriter
    :int32 'tech.v2.tensor.IntTensorWriter
    :int64 'tech.v2.tensor.LongTensorWriter
    :float32 'tech.v2.tensor.FloatTensorWriter
    :float64 'tech.v2.tensor.DoubleTensorWriter
    :boolean 'tech.v2.tensor.BooleanTensorWriter
    :object 'tech.v2.tensor.ObjectTensorWriter))


(defmacro implement-reader-cast
  [datatype item unchecked?]
  `(if (instance? ~(resolve (datatype->tensor-reader-type datatype)) ~item)
     ~item
     (tensor-proto/->tensor-reader ~item {:datatype ~datatype
                                          :unchecked? ~unchecked?})))



(defn ->int8-reader
  ^ByteTensorReader [item & [unchecked?]]
  (implement-reader-cast :int8 item unchecked?))
(defn ->int16-reader
  ^ShortTensorReader [item & [unchecked?]]
  (implement-reader-cast :int16 item unchecked?))
(defn ->int32-reader
  ^IntTensorReader [item & [unchecked?]]
  (implement-reader-cast :int32 item unchecked?))
(defn ->int64-reader
  ^LongTensorReader [item & [unchecked?]]
  (implement-reader-cast :int64 item unchecked?))
(defn ->float32-reader
  ^FloatTensorReader [item & [unchecked?]]
  (implement-reader-cast :float32 item unchecked?))
(defn ->float64-reader
  ^DoubleTensorReader [item & [unchecked?]]
  (implement-reader-cast :float64 item unchecked?))
(defn ->boolean-reader
  ^BooleanTensorReader [item & [unchecked?]]
  (implement-reader-cast :boolean item unchecked?))
(defn ->object-reader
  ^ObjectTensorReader [item & [unchecked?]]
  (implement-reader-cast :object item unchecked?))


(defmacro datatype->tensor-reader
  "Convert an item into a know tensor reader type."
  [datatype item & [unchecked?]]
  (case (casting/safe-flatten datatype)
    :int8 `(->int8-reader ~item ~unchecked?)
    :int16 `(->int16-reader ~item ~unchecked?)
    :int32 `(->int32-reader ~item ~unchecked?)
    :int64 `(->int64-reader ~item ~unchecked?)
    :float32 `(->float32-reader ~item ~unchecked?)
    :float64 `(->float64-reader ~item ~unchecked?)
    :boolean `(->boolean-reader ~item ~unchecked?)
    :object `(->object-reader ~item ~unchecked?)))


(defmacro implement-writer-cast
  [datatype item unchecked?]
  `(if (instance? ~(resolve (datatype->tensor-writer-type datatype)) ~item)
     ~item
     (tensor-proto/->tensor-writer ~item {:datatype ~datatype
                                          :unchecked? ~unchecked?})))



(defn ->int8-writer
  ^ByteTensorWriter [item & [unchecked?]]
  (implement-writer-cast :int8 item unchecked?))
(defn ->int16-writer
  ^ShortTensorWriter [item & [unchecked?]]
  (implement-writer-cast :int16 item unchecked?))
(defn ->int32-writer
  ^IntTensorWriter [item & [unchecked?]]
  (implement-writer-cast :int32 item unchecked?))
(defn ->int64-writer
  ^LongTensorWriter [item & [unchecked?]]
  (implement-writer-cast :int64 item unchecked?))
(defn ->float32-writer
  ^FloatTensorWriter [item & [unchecked?]]
  (implement-writer-cast :float32 item unchecked?))
(defn ->float64-writer
  ^DoubleTensorWriter [item & [unchecked?]]
  (implement-writer-cast :float64 item unchecked?))
(defn ->boolean-writer
  ^BooleanTensorWriter [item & [unchecked?]]
  (implement-writer-cast :boolean item unchecked?))
(defn ->object-writer
  ^ObjectTensorWriter [item & [unchecked?]]
  (implement-writer-cast :object item unchecked?))


(defmacro datatype->tensor-writer
  "Convert an item into a know tensor writer type."
  [datatype item & [unchecked?]]
  (case (casting/safe-flatten datatype)
    :int8 `(->int8-writer ~item ~unchecked?)
    :int16 `(->int16-writer ~item ~unchecked?)
    :int32 `(->int32-writer ~item ~unchecked?)
    :int64 `(->int64-writer ~item ~unchecked?)
    :float32 `(->float32-writer ~item ~unchecked?)
    :float64 `(->float64-writer ~item ~unchecked?)
    :boolean `(->boolean-writer ~item ~unchecked?)
    :object `(->object-writer ~item ~unchecked?)))
