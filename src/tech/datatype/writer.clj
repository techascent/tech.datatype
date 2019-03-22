(ns tech.datatype.writer
  (:require [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.nio-access
             :refer [buf-put buf-get
                     datatype->pos-fn
                     datatype->read-fn
                     datatype->write-fn
                     unchecked-full-cast
                     checked-full-read-cast
                     checked-full-write-cast
                     nio-type? list-type?
                     cls-type->read-fn
                     cls-type->write-fn
                     cls-type->pos-fn]]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.parallel :as parallel]
            [tech.jna :as jna]
            [clojure.core.matrix :as m]
            [tech.datatype.typecast :refer :all]
            [tech.datatype.fast-copy :as fast-copy]
            [tech.datatype.io :as dtype-io]
            [tech.datatype.typecast :as typecast])
  (:import [tech.datatype ObjectWriter ByteWriter
            ShortWriter IntWriter LongWriter
            FloatWriter DoubleWriter BooleanWriter]
           [com.sun.jna Pointer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn ecount
  "Type hinted ecount."
  ^long [item]
  (m/ecount item))


(declare ->marshalling-writer)


(jna/def-jna-fn "c" memset
  "Set a block of memory to a value"
  Pointer
  [data ensure-ptr-like]
  [val int]
  [num-bytes int])


(defmacro make-buffer-writer
  "Make a writer from a nio buffer or a fastutil list backing store.  "
  [writer-type buffer-type buffer
   writer-datatype
   intermediate-datatype
   buffer-datatype
   unchecked?]
  `(if ~unchecked?
     (reify ~writer-type
       (getDatatype [writer#] ~intermediate-datatype)
       (write [writer# idx# value#]
         (cls-type->write-fn ~buffer-type ~buffer idx#
          (cls-type->pos-fn ~buffer-type ~buffer)
          (unchecked-full-cast value# ~writer-datatype
                               ~intermediate-datatype
                               ~buffer-datatype))))
     (reify ~writer-type
       (write [writer# idx# value#]
         (cls-type->write-fn ~buffer-type ~buffer idx#
                             (cls-type->pos-fn ~buffer-type ~buffer)
                             (checked-full-write-cast value# ~writer-datatype
                                                  ~intermediate-datatype
                                                  ~buffer-datatype))))))


(defmacro make-marshalling-writer
  [dst-writer result-dtype intermediate-dtype src-dtype src-writer-type
   unchecked?]
  `(if ~unchecked?
     (reify ~src-writer-type
       (getDatatype [item#] ~intermediate-dtype)
       (write[item# idx# value#]
         (.write ~dst-writer idx#
                 (unchecked-full-cast value# ~src-dtype ~intermediate-dtype ~result-dtype))))
     (reify ~src-writer-type
       (write[item# idx# value#]
         (.write ~dst-writer idx#
                 (checked-full-write-cast value# ~src-dtype ~intermediate-dtype ~result-dtype))))))



(defmacro extend-writer-type
  [writer-type datatype]
  `(clojure.core/extend
       ~writer-type
     dtype-proto/PToWriter
     {:->writer-of-type
      (fn [item# dtype# unchecked?#]
        (let [dst-writer# (datatype->writer ~datatype item# true)]
          (if (= dtype# ~datatype)
            item#
            (case dtype#
              :int8 (make-marshalling-writer dst-writer# ~datatype
                                             :int8 :int8 ByteWriter unchecked?#)
              :uint8 (make-marshalling-writer dst-writer# ~datatype
                                              :uint8 :int16 ShortWriter unchecked?#)
              :int16 (make-marshalling-writer dst-writer# ~datatype
                                              :int16 :int16 ShortWriter unchecked?#)
              :uint16 (make-marshalling-writer dst-writer# ~datatype
                                               :uint16 :int32 IntWriter unchecked?#)
              :int32 (make-marshalling-writer dst-writer# ~datatype
                                              :int32 :int32 IntWriter unchecked?#)
              :uint32 (make-marshalling-writer dst-writer# ~datatype
                                               :uint32 :int64 LongWriter unchecked?#)
              :int64 (make-marshalling-writer dst-writer# ~datatype
                                              :int64 :int64 LongWriter unchecked?#)
              :uint64 (make-marshalling-writer dst-writer# ~datatype
                                               :uint64 :int64 LongWriter unchecked?#)
              :float32 (make-marshalling-writer dst-writer# ~datatype
                                                :float32 :float32 FloatWriter unchecked?#)
              :float64 (make-marshalling-writer dst-writer# ~datatype
                                                :float64 :float64 DoubleWriter unchecked?#)
              :boolean (make-marshalling-writer dst-writer# ~datatype
                                                :boolean :boolean BooleanWriter
                                                unchecked?#)
              :object (make-marshalling-writer dst-writer# ~datatype
                                               :object :object ObjectWriter
                                               unchecked?#)))))}))


(extend-writer-type ByteWriter :int8)
(extend-writer-type ShortWriter :int16)
(extend-writer-type IntWriter :int32)
(extend-writer-type LongWriter :int64)
(extend-writer-type FloatWriter :float32)
(extend-writer-type DoubleWriter :float64)
(extend-writer-type BooleanWriter :boolean)
(extend-writer-type ObjectWriter :object)
