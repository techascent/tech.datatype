(ns tech.datatype.nio-buffer
  (:require [tech.jna :as jna]
            [tech.datatype.io :as dtype-io]
            [tech.datatype.base :as base]
            [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [clojure.core.matrix.protocols :as mp]
            [tech.datatype.reader :as reader]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.parallel :as parallel])
  (:import [com.sun.jna Pointer]
           [java.nio Buffer ByteBuffer ShortBuffer
            IntBuffer LongBuffer FloatBuffer DoubleBuffer]
           [tech.datatype
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


(defn as-byte-buffer
  ^ByteBuffer [obj] obj)

(defn as-short-buffer
  ^ShortBuffer [obj] obj)

(defn as-int-buffer
  ^IntBuffer [obj] obj)

(defn as-long-buffer
  ^LongBuffer [obj] obj)

(defn as-float-buffer
  ^FloatBuffer [obj] obj)

(defn as-double-buffer
  ^DoubleBuffer [obj] obj)



(defmacro datatype->buffer-cast-fn
  [dtype buf]
  (condp = dtype
    :int8 `(as-byte-buffer ~buf)
    :int16 `(as-short-buffer ~buf)
    :int32 `(as-int-buffer ~buf)
    :int64 `(as-long-buffer ~buf)
    :float32 `(as-float-buffer ~buf)
    :float64 `(as-double-buffer ~buf)))


(defmacro datatype->buffer-creation
  [datatype src-ary]
  (case datatype
    :int8 `(ByteBuffer/wrap ^bytes ~src-ary)
    :int16 `(ShortBuffer/wrap ^shorts ~src-ary)
    :int32 `(IntBuffer/wrap ^ints ~src-ary)
    :int64 `(LongBuffer/wrap ^longs ~src-ary)
    :float32 `(FloatBuffer/wrap ^floats ~src-ary)
    :float64 `(DoubleBuffer/wrap ^doubles ~src-ary)))


(jna/def-jna-fn "c" memset
  "Set a block of memory to a value"
  Pointer
  [data dtype-io/ensure-ptr-like]
  [val int]
  [num-bytes int])


(defn memset-constant
  "Try to memset a constant value.  Returns true if succeeds, false otherwise"
  [item offset value elem-count]
  (let [offset (long offset)
        elem-count (long elem-count)]
    (if (or (= 0.0 (double value))
            (and (<= Byte/MAX_VALUE (long value))
                 (>= Byte/MIN_VALUE (long value))
                 (= :int8 (dtype-proto/get-datatype item))))
      (do
        (when-not (<= (+ (long offset)
                         (long elem-count))
                      (base/ecount item))
          (throw (ex-info "Memset out of range"
                          {:offset offset
                           :elem-count elem-count
                           :item-ecount (base/ecount item)})))
        (memset (dtype-proto/sub-buffer item offset elem-count)
                (int value)
                (* elem-count (base/datatype->byte-size
                               (dtype-proto/get-datatype item))))
        true)
      false)))


(extend-type Buffer
  dtype-proto/PContainerType
  (container-type [item] :nio-buffer)
  (dense-container? [item] true)
  (sparse-container? [item] false)
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


(defmacro make-buffer-writer
  [writer-type buffer datatype]
  `(reify ~writer-type
     (write [writer# idx# value#]
       (.put ~buffer (+ idx# (.position ~buffer)) value#))
     (writeConstant [writer# idx# value# count#]
       (let [value# (casting/datatype->cast-fn :unknown ~datatype value#)
             zero-val# (casting/datatype->unchecked-cast-fn :unkown ~datatype 0)]
         (if (or (= ~datatype :int8)
                 (= ~datatype :uint8)
                 (= value# zero-val#))
           ;;For zero hit fast path
           (memset (dtype-proto/sub-buffer ~buffer idx# count#)
                   (int value#) count#)
           (let [pos# (.position ~buffer)]
             (c-for [idx# (int 0) (< idx# count#) (inc idx#)]
                    (.put ~buffer (+ idx# pos#) value#))))))
     (writeBlock [writer# offset# values#]
       (dtype-io/dense-copy! (dtype-proto/sub-buffer ~buffer offset# (base/ecount values#))
                             values# (base/ecount values#) true true))
     (writeIndexes [writer# indexes# values#]
       (let [n-elems# (base/ecount indexes#)
             buf-pos# (.position ~buffer)
             idx-pos# (.position indexes#)
             val-pos# (.position values#)]
         (parallel/parallel-for
          idx#
          n-elems#
          (.put ~buffer (+ buf-pos#
                           (.get indexes# (+ idx-pos# idx#)))
                (.get values# (+ idx# val-pos#))))))))


(defmacro make-buffer-reader
  [reader-type buffer datatype]
  `(reify ~reader-type
     (read [reader# idx#]
       (.get ~buffer (+ idx# (.position ~buffer))))
     (readBlock [reader# offset# dest#]
       (dtype-io/dense-copy! dest# (dtype-proto/sub-buffer ~buffer offset# (base/ecount dest#))
                             (base/ecount dest#) true true))
     (readIndexes [reader# indexes# dest#]
       (let [idx-pos# (.position indexes#)
             dest-pos# (.position dest#)
             buf-pos# (.position ~buffer)
             n-elems# (base/ecount dest#)]
         (parallel/parallel-for
          idx#
          n-elems#
          (.put dest# (+ idx# dest-pos#)
                (.get ~buffer (+ buf-pos#
                                 (.get indexes# (+ idx# idx-pos#))))))))))


(defmacro implement-buffer-type
  [buffer-class datatype]
  `(clojure.core/extend
       ~buffer-class
     dtype-proto/PDatatype
     {:get-datatype (fn [arg#] ~datatype)}
     dtype-proto/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (let [copy-len# (.remaining (datatype->buffer-cast-fn
                                                      ~datatype raw-data#))]
                           (base/copy! raw-data# 0 ary-target# target-offset#
                                       copy-len# options#)
                           [ary-target# (+ (long target-offset#) copy-len#)]))}
     dtype-proto/PPrototype
     {:from-prototype (fn [src-ary# datatype# shape#]
                        (if-not (.isDirect (datatype->buffer-cast-fn ~datatype src-ary#))
                          (make-buffer-of-type datatype# (base/shape->ecount shape#))
                          (throw (ex-info "Cannot clone direct nio buffers" {}))))}
     dtype-proto/PToNioBuffer
     {:->buffer-backing-store (fn [item#] item#)}

     dtype-proto/PToArray
     {:->array (fn [item#]
                 (let [item# (datatype->buffer-cast-fn ~datatype item#)]
                   (when (and (= 0 (.position item#))
                              (not (.isDirect item#)))
                     (let [array-data# (.array item#)]
                       (when (= (.limit item#)
                                (alength array-data#))
                         array-data#)))))
      :->sub-array (fn [item#]
                     (let [item# (datatype->buffer-cast-fn ~datatype item#)]
                       (when-not (.isDirect item#)
                         {:array-data (.array item#)
                          :offset (.position item#)
                          :length (mp/element-count item#)})))
      :->array-copy (fn [item#]
                      (let [dst-ary# (base/make-container
                                      :jvm-array ~datatype
                                      (mp/element-count item#))]
                        (base/copy! item# dst-ary#)))}
     dtype-proto/PNioBuffer
     {:position (fn [item#] (.position (datatype->buffer-cast-fn ~datatype item#)))
      :limit (fn [item#] (.limit (datatype->buffer-cast-fn ~datatype item#)))
      :array-backed? (fn [item#] (not (.isDirect (datatype->buffer-cast-fn ~datatype item#))))}

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
                          (identical? (:array-data (dtype-proto/->sub-array lhs-buffer#))
                                      (:array-data (dtype-proto/->sub-array rhs-buffer#)))))))))
      :partially-alias? (fn [lhs-buffer# rhs-buffer#]
                          (when-let [nio-buf# (dtype-proto/->buffer-backing-store rhs-buffer#)]
                            (when (and (= (base/get-datatype lhs-buffer#)
                                          (base/get-datatype rhs-buffer#))
                                       (= (base/ecount lhs-buffer#)
                                          (base/ecount rhs-buffer#)))
                              (let [lhs-buffer# (datatype->buffer-cast-fn ~datatype lhs-buffer#)
                                    rhs-buffer# (datatype->buffer-cast-fn ~datatype nio-buf#)]
                                (when (= (.isDirect lhs-buffer#) (.isDirect rhs-buffer#))
                                  (if (.isDirect lhs-buffer#)
                                    (let [lhs-ptr# (Pointer/nativeValue ^Pointer (jna/->ptr-backing-store lhs-buffer#))
                                          rhs-ptr# (Pointer/nativeValue ^Pointer (jna/->ptr-backing-store rhs-buffer#))]
                                      (in-range? lhs-ptr# (base/ecount lhs-buffer#)
                                                 rhs-ptr# (base/ecount rhs-buffer#)))
                                    (let [lhs-sub# (dtype-proto/->sub-array lhs-buffer#)
                                          rhs-sub# (dtype-proto/->sub-array rhs-buffer#)]
                                      (and (identical? (:array-data lhs-sub#)
                                                       (:array-data rhs-sub#))
                                           (in-range? (:offset lhs-sub#) (:length lhs-sub#)
                                                      (:offset rhs-sub#) (:length rhs-sub#))))))))))}
     dtype-proto/PToWriter
     {:->object-writer (fn [item#]
                         (dtype-io/make-object-writer item# ~datatype))
      :->writer-of-type (fn [item# writer-datatype#]
                          (when-not (= writer-datatype# ~datatype)
                            (throw (ex-info "Base containers cannot marshal on write or read"
                                            {:datatype ~datatype
                                             :write-datatype writer-datatype#})))
                          (let [~'buffer (datatype->buffer-cast-fn ~datatype item#)]
                            ~(case datatype
                               :int8 `(make-buffer-writer ByteWriter ~'buffer :int8)
                               :int16 `(make-buffer-writer ShortWriter ~'buffer :int16)
                               :int32 `(make-buffer-writer IntWriter ~'buffer :int32)
                               :int64 `(make-buffer-writer LongWriter ~'buffer :int64)
                               :float32 `(make-buffer-writer FloatWriter ~'buffer :float32)
                               :float64 `(make-buffer-writer DoubleWriter ~'buffer :float64))))}
     dtype-proto/PToReader
     {:->object-reader (fn [item#]
                         (dtype-io/make-object-reader item# ~datatype))

      :->reader-of-type (fn [item# reader-datatype# unchecked?#]
                          (if (= reader-datatype# ~datatype)
                            (let [~'buffer (datatype->buffer-cast-fn ~datatype item#)]
                              ~(case datatype
                                 :int8 `(make-buffer-reader ByteReader ~'buffer :int8)
                                 :int16 `(make-buffer-reader ShortReader ~'buffer :int16)
                                 :int32 `(make-buffer-reader IntReader ~'buffer :int32)
                                 :int64 `(make-buffer-reader LongReader ~'buffer :int64)
                                 :float32 `(make-buffer-reader FloatReader ~'buffer :float32)
                                 :float64 `(make-buffer-reader DoubleReader ~'buffer :float64)))
                            (reader/make-reader item# reader-datatype# unchecked?#)))}))


(implement-buffer-type ByteBuffer :int8)
