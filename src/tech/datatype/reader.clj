(ns tech.datatype.reader
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting :as casting]
            [tech.parallel :as parallel]
            [tech.jna :as jna]
            [tech.datatype.nio-access :refer [buf-put buf-get]]
            [clojure.core.matrix.macros :refer [c-for]]
            [clojure.core.matrix.protocols :as mp]
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


(defmacro reader-type->datatype
  [reader-type]
  (case reader-type
    ObjectReader `:object
    ByteReader `:int8
    ShortReader `:int16
    IntReader `:int32
    LongReader `:int64
    FloatReader `:float32
    DoubleReader `:float64
    BooleanReader `:boolean))


(defmacro datatype->reader-type
  [datatype]
  (case datatype
    :int8 `ByteReader
    :uint8 `ShortReader
    :int16 `ShortReader
    :uint16 `IntReader
    :int32 `IntReader
    :uint32 `LongReader
    :int64 `LongReader
    :uint64 `LongReader
    :float32 `FloatReader
    :float64 `DoubleReader
    :boolean `BooleanReader
    :object `ObjectReader))


(defn ->object-reader ^ObjectReader [item]
  (if (instance? ObjectReader item)
    item
    (dtype-proto/->object-reader item)))


(defmacro implement-reader-cast
  [datatype]
  `(if (instance? (datatype->reader-type ~datatype) ~'item)
     ~'item
     (dtype-proto/->reader-of-type ~'item ~datatype ~'unchecked?)))


(defn ->byte-reader ^ByteReader [item unchecked?] (implement-reader-cast :int8))
(defn ->short-reader ^ShortReader [item unchecked?] (implement-reader-cast :int16))
(defn ->int-reader ^IntReader [item unchecked?] (implement-reader-cast :int32))
(defn ->long-reader ^LongReader [item unchecked?] (implement-reader-cast :int64))
(defn ->float-reader ^FloatReader [item unchecked?] (implement-reader-cast :float32))
(defn ->double-reader ^DoubleReader [item unchecked?] (implement-reader-cast :float64))
(defn ->boolean-reader ^BooleanReader [item unchecked?] (implement-reader-cast :boolean))


(defmacro datatype->reader
  [datatype reader unchecked?]
  (case datatype
    :int8 `(->byte-reader ~reader ~unchecked?)
    :uint8 `(->short-reader ~reader ~unchecked?)
    :int16 `(->short-reader ~reader ~unchecked?)
    :uint16 `(->int-reader ~reader ~unchecked?)
    :int32 `(->int-reader ~reader ~unchecked?)
    :uint32 `(->long-reader ~reader ~unchecked?)
    :int64 `(->long-reader ~reader ~unchecked?)
    :uint64 `(->long-reader ~reader ~unchecked?)
    :float32 `(->float-reader ~reader ~unchecked?)
    :float64 `(->double-reader ~reader ~unchecked?)
    :boolean `(->boolean-reader ~reader ~unchecked?)
    :object `(->object-reader ~reader ~unchecked?)))


(extend-type ObjectReader
  dtype-proto/PDatatype
  (get-datatype [_] :object)

  dtype-proto/PToReader
  (->object-reader [item] item)
  (->reader-of-type [item dtype unchecked?]
    (throw (ex-info "Cannot convert object readers to other readers." {}))))


(defmacro make-marshalling-reader
  [src-reader src-dtype dst-dtype reader-dtype dst-reader-type unchecked?]
  `(let [~'src-reader (datatype->reader ~src-dtype ~src-reader ~unchecked?)]
     ~(if (casting/numeric-type? dst-dtype)
        `(if ~unchecked?
           (reify ~dst-reader-type
             (read [item# idx#]
               (casting/datatype->unchecked-cast-fn
                ~dst-dtype
                ~reader-dtype
                (casting/datatype->unchecked-cast-fn
                 ~src-dtype ~dst-dtype
                 (.read ~'src-reader idx#))))
             (readBlock [item# offset# dest#]
               (let [n-elems# (ecount dest#)
                     dest-pos# (.position dest#)]
                 (parallel/parallel-for
                  idx# n-elems#
                  (buf-put dest# idx# dest-pos#
                        (.read item# (+ offset# idx#))))))
             (readIndexes [item# indexes# dest#]
               (let [n-elems# (ecount dest#)
                     dest-pos# (.position dest#)
                     idx-pos# (.position indexes#)]
                 (parallel/parallel-for
                  idx# n-elems#
                  (buf-put dest# idx# dest-pos#
                        (.read item# (.get indexes#
                                           (+ idx# idx-pos#))))))))
           (reify ~dst-reader-type
             (read [item# idx#]
               (casting/datatype->cast-fn
                ~dst-dtype
                ~reader-dtype
                (casting/datatype->cast-fn
                 ~src-dtype ~dst-dtype
                 (.read ~'src-reader idx#))))
             (readBlock [item# offset# dest#]
               (let [n-elems# (ecount dest#)
                     dest-pos# (.position dest#)]
                 (parallel/parallel-for
                  idx# n-elems#
                  (buf-put dest# idx# dest-pos#
                           (.read item# (+ offset# idx#))))))
             (readIndexes [item# indexes# dest#]
               (let [n-elems# (ecount dest#)
                     dest-pos# (.position dest#)
                     idx-pos# (.position indexes#)]
                 (parallel/parallel-for
                  idx# n-elems#
                  (buf-put dest# idx# dest-pos#
                           (.read item# (buf-get indexes#
                                                 idx# idx-pos#))))))))
        (if (= :boolean dst-dtype)
          `(reify ~dst-reader-type
             (read [item# idx#]
               (casting/datatype->unchecked-cast-fn
                ~src-dtype ~dst-dtype
                (.read ~'src-reader idx#)))
             (readBlock [item# offset# dest#]
               (let [n-elems# (ecount dest#)]
                 (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                        (.set dest# idx#
                              (.read item# (+ offset# idx#))))))
             (readIndexes [item# indexes# dest#]
               (let [n-elems# (ecount dest#)
                     idx-pos# (.position indexes#)]
                 (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                        (.set dest# idx#
                              (.read item# (buf-get indexes#
                                                    idx# idx-pos#)))))))
          `(reify ~dst-reader-type
             (read [item# idx#]
               (.read ~'src-reader idx#))
             (readBlock [item# offset# dest#]
               (let [n-elems# (ecount dest#)]
                 (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                        (.set dest# idx#
                              (.read item# (+ offset# idx#))))))
             (readIndexes [item# indexes# dest#]
               (let [n-elems# (ecount dest#)
                     idx-pos# (.position indexes#)]
                 (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                        (.set dest# idx#
                              (.read item# (buf-get indexes#
                                                    idx# idx-pos#)))))))))))


(defmacro extend-reader-type
  [reader-type datatype]
  `(clojure.core/extend
       ~reader-type
     dtype-proto/PDatatype
     {:get-datatype (fn [_#] ~datatype)}
     dtype-proto/PToReader
     {:->object-reader (fn [item#]
                         (make-marshalling-reader item# ~datatype
                                                  :object :object ObjectReader true))
      :->reader-of-type
      (fn [item# dtype# unchecked?#]
        (if (= dtype# ~datatype)
          item#
          (case dtype#
            :int8 (make-marshalling-reader item# ~datatype
                                           :int8 :int8 ByteReader unchecked?#)
            :uint8 (make-marshalling-reader item# ~datatype
                                            :uint8 :int16 ShortReader unchecked?#)
            :int16 (make-marshalling-reader item# ~datatype
                                            :int16 :int16 ShortReader unchecked?#)
            :uint16 (make-marshalling-reader item# ~datatype
                                             :uint16 :int32 IntReader unchecked?#)
            :int32 (make-marshalling-reader item# ~datatype
                                            :int32 :int32 IntReader unchecked?#)
            :uint32 (make-marshalling-reader item# ~datatype
                                             :uint32 :int64 LongReader unchecked?#)
            :int64 (make-marshalling-reader item# ~datatype
                                            :int64 :int64 LongReader unchecked?#)
            :uint64 (make-marshalling-reader item# ~datatype
                                             :uint64 :int64 LongReader unchecked?#)
            :float32 (make-marshalling-reader item# ~datatype
                                              :float32 :float32 FloatReader unchecked?#)
            :float64 (make-marshalling-reader item# ~datatype
                                              :float64 :float64 DoubleReader unchecked?#)
            :boolean (make-marshalling-reader item# ~datatype
                                              :boolean :boolean BooleanReader unchecked?#)
            :object (make-marshalling-reader item# ~datatype
                                             :object :object ObjectReader unchecked?#))))}))


(extend-reader-type ByteReader :int8)
(extend-reader-type ShortReader :int16)
(extend-reader-type IntReader :int32)
(extend-reader-type LongReader :int64)
(extend-reader-type FloatReader :float32)
(extend-reader-type DoubleReader :float64)
(extend-reader-type BooleanReader :boolean)


(defn ->marshalling-reader
  [src-item dest-dtype unchecked?]
  (let [src-dtype (dtype-proto/get-datatype src-item)
        src-reader (dtype-proto/->reader-of-type src-item src-dtype false)]
    (if (= src-dtype dest-dtype)
      src-reader
      (dtype-proto/->reader-of-type src-reader dest-dtype unchecked?))))
