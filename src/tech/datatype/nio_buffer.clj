(ns tech.datatype.nio-buffer
  "Nio buffers really are the workhorses of the entire system."
  (:require [tech.jna :as jna]
            [tech.datatype.io :as dtype-io]
            [tech.datatype.base :as base]
            [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.typecast :refer :all]
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
            [clojure.core.matrix.protocols :as mp]
            [tech.datatype.reader :refer [make-buffer-reader] :as reader]
            [tech.datatype.writer :refer [make-buffer-writer] :as writer]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.parallel :as parallel]
            [tech.datatype.array])
  (:import [com.sun.jna Pointer]
           [java.nio Buffer ByteBuffer ShortBuffer
            IntBuffer LongBuffer FloatBuffer DoubleBuffer]
           [tech.datatype
            ObjectReader ObjectWriter ObjectMutable
            ByteReader ByteWriter ByteMutable
            ShortReader ShortWriter ShortMutable
            IntReader IntWriter IntMutable
            LongReader LongWriter LongMutable
            FloatReader FloatWriter FloatMutable
            DoubleReader DoubleWriter DoubleMutable
            BooleanReader BooleanWriter BooleanMutable]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(extend-type Buffer
  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (base/raw-dtype-copy! raw-data ary-target target-offset options))
  mp/PElementCount
  (element-count [item] (.remaining item)))


(declare make-buffer-of-type)


(defn in-range?
  [^long lhs-off ^long lhs-len ^long rhs-off ^long rhs-len]
    (or (and (>= rhs-off lhs-off)
           (< rhs-off (+ lhs-off lhs-len)))
      (and (>= lhs-off rhs-off)
           (< lhs-off (+ rhs-off rhs-len)))))




(defmacro implement-buffer-type
  [buffer-class datatype]
  `(clojure.core/extend
       ~buffer-class
     dtype-proto/PDatatype
     {:get-datatype (fn [arg#] ~datatype)}
     dtype-proto/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (base/raw-dtype-copy! raw-data# ary-target#
                                               target-offset# options#))}
     dtype-proto/PPrototype
     {:from-prototype (fn [src-ary# datatype# shape#]
                        (if-not (.isDirect (datatype->buffer-cast-fn ~datatype src-ary#))
                          (make-buffer-of-type datatype# (base/shape->ecount shape#))
                          (throw (ex-info "Cannot clone direct nio buffers" {}))))}
     dtype-proto/PToNioBuffer
     {:->buffer-backing-store (fn [item#] item#)}

     dtype-proto/PToArray
     {:->sub-array (fn [item#]
                     (let [item# (datatype->buffer-cast-fn ~datatype item#)]
                       (when-not (.isDirect item#)
                         {:array-data (.array item#)
                          :offset (.position item#)
                          :length (mp/element-count item#)})))
      :->array-copy (fn [item#]
                      (let [dst-ary# (base/make-container
                                      :java-array ~datatype
                                      (mp/element-count item#))]
                        (base/copy! item# dst-ary#)))}
     dtype-proto/PNioBuffer
     {:position (fn [item#] (.position (datatype->buffer-cast-fn ~datatype item#)))
      :limit (fn [item#] (.limit (datatype->buffer-cast-fn ~datatype item#)))
      :array-backed? (fn [item#] (not (.isDirect (datatype->buffer-cast-fn
                                                  ~datatype item#))))}

     dtype-proto/PBuffer
     {:sub-buffer (fn [buffer# offset# length#]
                    (let [buf# (.slice (datatype->buffer-cast-fn ~datatype buffer#))
                          offset# (long offset#)
                          len# (long length#)]
                      (.position buf# offset#)
                      (.limit buf# (+ offset# len#))
                      buf#))
      :alias? (fn [lhs-buffer# rhs-buffer#]
                (when-let [nio-buf# (dtype-proto/->buffer-backing-store rhs-buffer#)]
                  (when (and
                             (= (base/get-datatype lhs-buffer#)
                                (base/get-datatype rhs-buffer#))
                             (= (base/ecount lhs-buffer#)
                                (base/ecount rhs-buffer#)))
                    (let [lhs-buffer# (datatype->buffer-cast-fn ~datatype lhs-buffer#)
                          rhs-buffer# (datatype->buffer-cast-fn ~datatype nio-buf#)]
                      (when (= (.isDirect lhs-buffer#) (.isDirect rhs-buffer#))
                        (if (.isDirect lhs-buffer#)
                          (= (jna/->ptr-backing-store lhs-buffer#)
                             (jna/->ptr-backing-store rhs-buffer#))
                          (identical? (:array-data (dtype-proto/->sub-array
                                                    lhs-buffer#))
                                      (:array-data (dtype-proto/->sub-array
                                                    rhs-buffer#)))))))))
      :partially-alias?
      (fn [lhs-buffer# rhs-buffer#]
        (when-let [nio-buf# (dtype-proto/->buffer-backing-store
                             rhs-buffer#)]
          (when (and (= (base/get-datatype lhs-buffer#)
                        (base/get-datatype rhs-buffer#))
                     (= (base/ecount lhs-buffer#)
                        (base/ecount rhs-buffer#)))
            (let [lhs-buffer# (datatype->buffer-cast-fn ~datatype lhs-buffer#)
                  rhs-buffer# (datatype->buffer-cast-fn ~datatype nio-buf#)]
              (when (= (.isDirect lhs-buffer#) (.isDirect rhs-buffer#))
                (if (.isDirect lhs-buffer#)
                  (let [lhs-ptr# (Pointer/nativeValue
                                  ^Pointer (jna/->ptr-backing-store lhs-buffer#))
                        rhs-ptr# (Pointer/nativeValue
                                  ^Pointer (jna/->ptr-backing-store rhs-buffer#))]
                    (in-range? lhs-ptr# (base/ecount lhs-buffer#)
                               rhs-ptr# (base/ecount rhs-buffer#)))
                  (let [lhs-sub# (dtype-proto/->sub-array lhs-buffer#)
                        rhs-sub# (dtype-proto/->sub-array rhs-buffer#)]
                    (and (identical? (:array-data lhs-sub#)
                                     (:array-data rhs-sub#))
                         (in-range? (:offset lhs-sub#)
                                    (:length lhs-sub#)
                                    (:offset rhs-sub#)
                                    (:length rhs-sub#))))))))))}
     dtype-proto/PToWriter
     {:->writer-of-type
      (fn [item# writer-datatype# unchecked?#]
        (let [~'buffer (datatype->buffer-cast-fn ~datatype item#)]
          (case writer-datatype#
            :int8 (make-buffer-writer ByteWriter ~buffer-class ~'buffer :int8
                                      :int8 ~datatype unchecked?#)
            :uint8 (make-buffer-writer ShortWriter ~buffer-class ~'buffer :int16
                                       :uint8 ~datatype unchecked?#)
            :int16 (make-buffer-writer ShortWriter ~buffer-class ~'buffer :int16
                                       :int16 ~datatype unchecked?#)
            :uint16 (make-buffer-writer IntWriter ~buffer-class ~'buffer :int32
                                        :uint16 ~datatype unchecked?#)
            :int32 (make-buffer-writer IntWriter ~buffer-class ~'buffer :int32
                                       :int32 ~datatype unchecked?#)
            :uint32 (make-buffer-writer LongWriter ~buffer-class ~'buffer :int64
                                        :uint32 ~datatype unchecked?#)
            :int64 (make-buffer-writer LongWriter ~buffer-class ~'buffer :int64
                                       :int64 ~datatype unchecked?#)
            :uint64 (make-buffer-writer LongWriter ~buffer-class ~'buffer :int64
                                        :int64 ~datatype unchecked?#)
            :float32 (make-buffer-writer FloatWriter ~buffer-class ~'buffer :float32
                                         :float32 ~datatype unchecked?#)
            :float64 (make-buffer-writer DoubleWriter ~buffer-class ~'buffer :float64
                                         :float64 ~datatype unchecked?#)
            :boolean (make-buffer-writer BooleanWriter ~buffer-class ~'buffer :boolean
                                         :boolean ~datatype unchecked?#)
            :object (make-buffer-writer ObjectWriter ~buffer-class ~'buffer :object
                                         :object ~datatype unchecked?#))))}
     dtype-proto/PToReader
     {:->reader-of-type
      (fn [item# reader-datatype# unchecked?#]
        (let [~'buffer (datatype->buffer-cast-fn ~datatype item#)]
          (case reader-datatype#
            :int8 (make-buffer-reader ByteReader ~buffer-class ~'buffer :int8
                                      :int8 ~datatype unchecked?#)
            :uint8 (make-buffer-reader ShortReader ~buffer-class ~'buffer :int16
                                       :uint8 ~datatype unchecked?#)
            :int16 (make-buffer-reader ShortReader ~buffer-class ~'buffer :int16
                                       :int16 ~datatype unchecked?#)
            :uint16 (make-buffer-reader IntReader ~buffer-class ~'buffer :int32
                                        :uint16 ~datatype unchecked?#)
            :int32 (make-buffer-reader IntReader ~buffer-class ~'buffer :int32
                                       :int32 ~datatype unchecked?#)
            :uint32 (make-buffer-reader LongReader ~buffer-class ~'buffer :int64
                                        :uint32 ~datatype unchecked?#)
            :int64 (make-buffer-reader LongReader ~buffer-class ~'buffer :int64
                                       :int64 ~datatype unchecked?#)
            :uint64 (make-buffer-reader LongReader ~buffer-class ~'buffer :int64
                                        :int64 ~datatype unchecked?#)
            :float32 (make-buffer-reader FloatReader ~buffer-class ~'buffer :float32
                                         :float32 ~datatype unchecked?#)
            :float64 (make-buffer-reader DoubleReader ~buffer-class ~'buffer :float64
                                         :float64 ~datatype unchecked?#)
            :boolean (make-buffer-reader BooleanReader ~buffer-class ~'buffer :boolean
                                         :boolean ~datatype unchecked?#)
            :object (make-buffer-reader ObjectReader ~buffer-class ~'buffer :object
                                        :object ~datatype unchecked?#))))}))


(implement-buffer-type ByteBuffer :int8)
(implement-buffer-type ShortBuffer :int16)
(implement-buffer-type IntBuffer :int32)
(implement-buffer-type LongBuffer :int64)
(implement-buffer-type FloatBuffer :float32)
(implement-buffer-type DoubleBuffer :float64)


(defn make-buffer-of-type
  ([datatype elem-count-or-seq options]
   (dtype-proto/->buffer-backing-store
    (dtype-proto/make-container :java-array datatype elem-count-or-seq options)))
  ([datatype elem-count-or-seq]
   (make-buffer-of-type datatype elem-count-or-seq {})))


(defmethod dtype-proto/make-container :nio-buffer
  [container-type datatype elem-count-or-seq options]
  (make-buffer-of-type datatype elem-count-or-seq options))
