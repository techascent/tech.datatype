(ns tech.datatype.writer
  (:require [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.nio-access :refer [buf-put buf-get
                                              datatype->pos-fn
                                              datatype->read-fn
                                              datatype->write-fn]]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.parallel :as parallel]
            [clojure.core.matrix :as m])
  (:import [tech.datatype
            ObjectReader ObjectWriter Mutable
            ByteReader ByteWriter ByteMutable
            ShortReader ShortWriter ShortMutable
            IntReader IntWriter IntMutable
            LongReader LongWriter LongMutable
            FloatReader FloatWriter FloatMutable
            DoubleReader DoubleWriter DoubleMutable
            BooleanReader BooleanWriter BooleanMutable]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn ecount
  "Type hinted ecount."
  ^long [item]
  (m/ecount item))


(defmacro writer-type->datatype
  [writer-type]
  (case writer-type
    ObjectWriter `:object
    ByteWriter `:int8
    ShortWriter `:int16
    IntWriter `:int32
    LongWriter `:int64
    FloatWriter `:float32
    DoubleWriter `:float64
    BooleanWriter `:boolean))


(defmacro datatype->writer-type
  [datatype]
  (case datatype
    :int8 `ByteWriter
    :uint8 `ShortWriter
    :int16 `ShortWriter
    :uint16 `IntWriter
    :int32 `IntWriter
    :uint32 `LongWriter
    :int64 `LongWriter
    :uint64 `LongWriter
    :float32 `FloatWriter
    :float64 `DoubleWriter
    :boolean `BooleanWriter
    :object `ObjectWriter))



(defmacro implement-writer-cast
  [datatype]
  `(if (instance? (datatype->writer-type ~datatype) ~'item)
     ~'item
     (dtype-proto/->writer-of-type ~'item ~datatype ~'unchecked?)))


(defn ->byte-writer ^ByteWriter [item unchecked?] (implement-writer-cast :int8))
(defn ->short-writer ^ShortWriter [item unchecked?] (implement-writer-cast :int16))
(defn ->int-writer ^IntWriter [item unchecked?] (implement-writer-cast :int32))
(defn ->long-writer ^LongWriter [item unchecked?] (implement-writer-cast :int64))
(defn ->float-writer ^FloatWriter [item unchecked?] (implement-writer-cast :float32))
(defn ->double-writer ^DoubleWriter [item unchecked?] (implement-writer-cast :float64))
(defn ->boolean-writer ^BooleanWriter [item unchecked?] (implement-writer-cast :boolean))
(defn ->object-writer ^ObjectWriter [item unchecked?] (implement-writer-cast :object))


(defmacro datatype->writer
  [datatype writer unchecked?]
  (case datatype
    :int8 `(->byte-writer ~writer ~unchecked?)
    :uint8 `(->short-writer ~writer ~unchecked?)
    :int16 `(->short-writer ~writer ~unchecked?)
    :uint16 `(->int-writer ~writer ~unchecked?)
    :int32 `(->int-writer ~writer ~unchecked?)
    :uint32 `(->long-writer ~writer ~unchecked?)
    :int64 `(->long-writer ~writer ~unchecked?)
    :uint64 `(->long-writer ~writer ~unchecked?)
    :float32 `(->float-writer ~writer ~unchecked?)
    :float64 `(->double-writer ~writer ~unchecked?)
    :boolean `(->boolean-writer ~writer ~unchecked?)
    :object `(->object-writer ~writer ~unchecked?)))

(declare ->marshalling-writer)



(defmacro make-marshalling-writer
  [dst-writer dst-dtype src-dtype src-writer-dtype src-writer-type
   unchecked?]
  `(let [~'dst-writer (datatype->writer ~dst-dtype ~dst-writer ~unchecked?)]
     (if ~unchecked?
       (reify ~src-writer-type
         (write[item# idx# value#]
           (.write ~'dst-writer idx#
                   (casting/datatype->unchecked-cast-fn
                    ~src-dtype
                    ~dst-dtype
                    (casting/datatype->unchecked-cast-fn
                     ~src-writer-dtype ~src-dtype value#))))
         (writeConstant [item# idx# value# count#]
           (.writeConstant ~'dst-writer idx#
                           (casting/datatype->unchecked-cast-fn
                            ~src-dtype
                            ~dst-dtype
                            (casting/datatype->unchecked-cast-fn
                             ~src-writer-dtype ~src-dtype value#))
                           count#))
         (writeBlock [item# offset# values#]
           (let [n-elems# (ecount values#)
                 values-pos# (datatype->pos-fn ~src-dtype values#)]
             (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                    (.write item# (+ offset# idx#)
                            (datatype->read-fn ~src-dtype values# idx# values-pos#)))))
         (writeIndexes [item# indexes# values#]
           (let [n-elems# (ecount values#)
                 values-pos# (datatype->pos-fn ~src-dtype values#)
                 idx-pos# (.position indexes#)]
             (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                    (.write item# (buf-get indexes#
                                           idx# idx-pos#)
                            (datatype->read-fn ~src-dtype values# values-pos# idx#))))))
       (reify ~src-writer-type
         (write[item# idx# value#]
           (.write ~'dst-writer idx#
                   (casting/datatype->unchecked-cast-fn
                    ~src-dtype
                    ~dst-dtype
                    (casting/datatype->cast-fn
                     ~src-writer-dtype ~src-dtype value#))))
         (writeConstant [item# idx# value# count#]
           (.writeConstant ~'dst-writer idx#
                           (casting/datatype->unchecked-cast-fn
                            ~src-dtype
                            ~dst-dtype
                            (casting/datatype->cast-fn
                             ~src-writer-dtype ~src-dtype value#))
                           count#))
         (writeBlock [item# offset# values#]
           (let [n-elems# (ecount values#)
                 values-pos# (datatype->pos-fn ~src-dtype values#)]
             (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                    (.write item# (+ offset# idx#)
                            (datatype->read-fn ~src-dtype values# idx# values-pos#)))))
         (writeIndexes [item# indexes# values#]
           (let [n-elems# (ecount values#)
                 values-pos# (datatype->pos-fn ~src-dtype values#)
                 idx-pos# (.position indexes#)]
             (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                    (.write item# (buf-get indexes#
                                           idx# idx-pos#)
                            (datatype->read-fn ~src-dtype values# idx# values-pos#)))))))))


(defmacro extend-writer-type
  [writer-type datatype]
  `(clojure.core/extend
       ~writer-type
     dtype-proto/PDatatype
     {:get-datatype (fn [_#] ~datatype)}
     dtype-proto/PToWriter
     {:->object-writer (fn [item#]
                         (make-marshalling-writer item# ~datatype
                                                  :object :object ObjectWriter true))
      :->writer-of-type
      (fn [item# dtype# unchecked?#]
        (if (= dtype# ~datatype)
          item#
          (case dtype#
            :int8 (make-marshalling-writer item# ~datatype
                                           :int8 :int8 ByteWriter unchecked?#)
            :uint8 (make-marshalling-writer item# ~datatype
                                            :uint8 :int16 ShortWriter unchecked?#)
            :int16 (make-marshalling-writer item# ~datatype
                                            :int16 :int16 ShortWriter unchecked?#)
            :uint16 (make-marshalling-writer item# ~datatype
                                             :uint16 :int32 IntWriter unchecked?#)
            :int32 (make-marshalling-writer item# ~datatype
                                            :int32 :int32 IntWriter unchecked?#)
            :uint32 (make-marshalling-writer item# ~datatype
                                             :uint32 :int64 LongWriter unchecked?#)
            :int64 (make-marshalling-writer item# ~datatype
                                            :int64 :int64 LongWriter unchecked?#)
            :uint64 (make-marshalling-writer item# ~datatype
                                             :uint64 :int64 LongWriter unchecked?#)
            :float32 (make-marshalling-writer item# ~datatype
                                              :float32 :float32 FloatWriter unchecked?#)
            :float64 (make-marshalling-writer item# ~datatype
                                              :float64 :float64 DoubleWriter unchecked?#)
            :boolean (make-marshalling-writer item# ~datatype
                                              :boolean :boolean BooleanWriter unchecked?#)
            :object (make-marshalling-writer item# ~datatype
                                             :object :object ObjectWriter unchecked?#))))}))


(extend-writer-type ByteWriter :int8)
(extend-writer-type ShortWriter :int16)
(extend-writer-type IntWriter :int32)
(extend-writer-type LongWriter :int64)
(extend-writer-type FloatWriter :float32)
(extend-writer-type DoubleWriter :float64)
(extend-writer-type BooleanWriter :boolean)
(extend-writer-type ObjectWriter :object)


(defn ->marshalling-writer
  [src-item dest-dtype unchecked?]
  (let [src-dtype (dtype-proto/get-datatype src-item)
        src-writer (dtype-proto/->writer-of-type src-item src-dtype false)]
    (if (= src-dtype dest-dtype)
      src-writer
      (dtype-proto/->writer-of-type src-writer dest-dtype unchecked?))))
