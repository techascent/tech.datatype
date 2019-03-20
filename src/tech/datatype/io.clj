(ns tech.datatype.io
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting
             :refer [numeric-type? integer-type? numeric-byte-width
                     datatype->jvm-type]
             :as casting]
            [tech.datatype.reader :as reader]
            [tech.datatype.writer :as writer]
            [tech.jna :as jna]
            [tech.parallel :as parallel]
            [clojure.set :as c-set]
            [clojure.core.matrix.macros :refer [c-for]])

  (:import [tech.datatype
            ObjectReader ObjectWriter ObjectMutable
            ByteReader ByteWriter ByteMutable
            ShortReader ShortWriter ShortMutable
            IntReader IntWriter IntMutable
            LongReader LongWriter LongMutable
            FloatReader FloatWriter FloatMutable
            DoubleReader DoubleWriter DoubleMutable
            BooleanReader BooleanWriter BooleanMutable]
           [com.sun.jna Pointer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(extend-protocol dtype-proto/PDatatype
  ObjectMutable
  (get-datatype [item] :object)
  ByteMutable
  (get-datatype [item] :int8)
  ShortMutable
  (get-datatype [item] :int16)
  IntMutable
  (get-datatype [item] :int32)
  LongMutable
  (get-datatype [item] :int64)
  FloatMutable
  (get-datatype [item] :float32)
  DoubleMutable
  (get-datatype [item] :float64)
  BooleanMutable
  (get-datatype [item] :boolean))

(defn ensure-ptr-like
  "JNA is extremely flexible in what it can take as an argument.  Anything convertible
  to a nio buffer, be it direct or array backend is fine."
  [item]
  (cond
    (satisfies? jna/PToPtr item)
    (jna/->ptr-backing-store item)
    :else
    (dtype-proto/->buffer-backing-store item)))

(jna/def-jna-fn "c" memcpy
  "Copy bytes from one object to another"
  Pointer
  [dst ensure-ptr-like]
  [src ensure-ptr-like]
  [n-bytes int])


(defn as-ptr
  ^Pointer [item]
  (when (satisfies? jna/PToPtr item)
    (jna/->ptr-backing-store item)))


(defn as-array
  [item]
  (when (satisfies? dtype-proto/PToArray item)
    (dtype-proto/->sub-array item)))


(defn as-nio-buffer
  [item]
  (when (satisfies? dtype-proto/PToNioBuffer item)
    (dtype-proto/->buffer-backing-store item)))



(defmacro datatype->parallel-write
  [datatype dst src n-elems unchecked?]
  `(let [writer# (writer/datatype->writer ~datatype ~dst true)
         reader# (reader/datatype->reader ~datatype ~src ~unchecked?)]
     (parallel/parallel-for
      idx#
      ~n-elems
      (.write writer# idx# (.read reader# idx#)))))


(defmacro datatype->serial-write
  [datatype dst src n-elems unchecked?]
  `(let [writer# (writer/datatype->writer ~datatype ~dst true)
         reader# (reader/datatype->reader ~datatype ~src ~unchecked?)
         n-elems# ~n-elems]
     (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
            (.write writer# idx# (.read reader# idx#)))))


(defn parallel-write
  [dst src n-elems unchecked?]
  (let [dst-dtype (dtype-proto/get-datatype dst)
        src-dtype (dtype-proto/get-datatype src)
        ;;Flatten all datatypes into the known set.
        op-type (if (or (numeric-type? dst-dtype)
                        (= :boolean dst-dtype))
                  dst-dtype
                  :object)
        n-elems (long n-elems)]
    (case op-type
      :int8 (datatype->parallel-write :int8 dst src n-elems unchecked?)
      :uint8 (datatype->parallel-write :uint8 dst src n-elems unchecked?)
      :int16 (datatype->parallel-write :int16 dst src n-elems unchecked?)
      :uint16 (datatype->parallel-write :uint16 dst src n-elems unchecked?)
      :int32 (datatype->parallel-write :int32 dst src n-elems unchecked?)
      :uint32 (datatype->parallel-write :uint32 dst src n-elems unchecked?)
      :int64 (datatype->parallel-write :int64 dst src n-elems unchecked?)
      :uint64 (datatype->parallel-write :uint64 dst src n-elems unchecked?)
      :float32 (datatype->parallel-write :float32 dst src n-elems unchecked?)
      :float64 (datatype->parallel-write :float64 dst src n-elems unchecked?))))


(defn serial-write
  [dst src n-elems unchecked?]
  (let [dst-dtype (dtype-proto/get-datatype dst)
        src-dtype (dtype-proto/get-datatype src)
        ;;Flatten all datatypes into the known set.
        op-type (if (or (numeric-type? dst-dtype)
                        (= :boolean dst-dtype))
                  dst-dtype
                  :object)
        n-elems (long n-elems)]
    (case op-type
      :int8 (datatype->serial-write :int8 dst src n-elems unchecked?)
      :uint8 (datatype->serial-write :uint8 dst src n-elems unchecked?)
      :int16 (datatype->serial-write :int16 dst src n-elems unchecked?)
      :uint16 (datatype->serial-write :uint16 dst src n-elems unchecked?)
      :int32 (datatype->serial-write :int32 dst src n-elems unchecked?)
      :uint32 (datatype->serial-write :uint32 dst src n-elems unchecked?)
      :int64 (datatype->serial-write :int64 dst src n-elems unchecked?)
      :uint64 (datatype->serial-write :uint64 dst src n-elems unchecked?)
      :float32 (datatype->serial-write :float32 dst src n-elems unchecked?)
      :float64 (datatype->serial-write :float64 dst src n-elems unchecked?))))


(defn dense-copy!
  [dst src n-elems unchecked? parallel?]
  (let [dst-dtype (dtype-proto/get-datatype dst)
        src-dtype (dtype-proto/get-datatype src)
        src-buf (as-nio-buffer src)
        dst-buf (as-nio-buffer dst)
        fast-path? (and src-buf
                        dst-buf
                        (or (and (numeric-type? dst-dtype)
                                 (= dst-dtype src-dtype))
                            (and (integer-type? dst-dtype)
                                 (integer-type? src-dtype)
                                 unchecked?
                                 (= (numeric-byte-width dst-dtype)
                                    (numeric-byte-width src-dtype)))))
        n-elems (long n-elems)]
    ;;Fast path means no conversion is necessary and we can hit optimized
    ;;bulk pathways
    (if fast-path?
      ;;The only real special case is if one side is a ptr
      ;;and the other is an array
      (let [dst-ptr (as-ptr dst)
            dst-ary (as-array dst)
            src-ptr (as-ptr src)
            src-ary (as-array src)
            ;;Flatten source so that it never represents the unsigned type.
            src-dtype (datatype->jvm-type src-dtype)]
        (cond
          ;;Very fast path
          (and dst-ptr src-ary)
          (let [{:keys [array-data array-offset]} src-ary
                array-offset (int array-offset)]
            (case src-dtype
              :int8 (.write dst-ptr 0 ^bytes array-data array-offset n-elems)
              :int16 (.write dst-ptr 0 ^shorts array-data array-offset n-elems)
              :int32 (.write dst-ptr 0 ^ints array-data array-offset n-elems)
              :int64 (.write dst-ptr 0 ^longs array-data array-offset n-elems)
              :float32 (.write dst-ptr 0 ^floats array-data array-offset n-elems)
              :float64 (.write dst-ptr 0 ^doubles array-data array-offset n-elems)))
          (and dst-ary src-ptr)
          (let [{:keys [array-data array-offset]} dst-ary
                array-offset (int array-offset)]
            (case src-dtype
              :int8 (.read src-ptr 0 ^bytes array-data array-offset n-elems)
              :int16 (.read src-ptr 0 ^shorts array-data array-offset n-elems)
              :int32 (.read src-ptr 0 ^ints array-data array-offset n-elems)
              :int64 (.read src-ptr 0 ^longs array-data array-offset n-elems)
              :float32 (.read src-ptr 0 ^floats array-data array-offset n-elems)
              :float64 (.read src-ptr 0 ^doubles array-data array-offset n-elems)))
          :else
          (memcpy dst-buf src-buf (* n-elems (numeric-byte-width src-dtype)))))
      ;;The slow path still has fast aspects if one of the buffers is a known primitive type.
      (let [unchecked-reads? (or unchecked?
                                 (= dst-dtype src-dtype))]
        (cond
          ;;Dest has a nio buffer *and* the nio buffer matches the datatype of dest.
          (and dst-buf (= dst-dtype (dtype-proto/get-datatype dst-buf)))
          (case dst-dtype
            :int8 (.readBlock (reader/datatype->reader :int8 src unchecked?) 0 dst-buf)
            :int16 (.readBlock (reader/datatype->reader :int16 src unchecked?) 0 dst-buf)
            :int32 (.readBlock (reader/datatype->reader :int32 src unchecked?) 0 dst-buf)
            :int64 (.readBlock (reader/datatype->reader :int64 src unchecked?) 0 dst-buf)
            :float32 (.readBlock (reader/datatype->reader :float32 src unchecked?) 0 dst-buf)
            :float64 (.readBlock (reader/datatype->reader :float64 src unchecked?) 0 dst-buf))
          ;;Src has nio buffer and nio buffer matches datatype or src
          (and src-buf (= src-dtype (dtype-proto/get-datatype src-buf)))
          (case src-dtype
            :int8 (.writeBlock (writer/datatype->writer :int8 dst unchecked?) 0 src-buf)
            :int16 (.writeBlock (writer/datatype->writer :int16 dst unchecked?) 0 src-buf)
            :int32 (.writeBlock (writer/datatype->writer :int32 dst unchecked?) 0 src-buf)
            :int64 (.writeBlock (writer/datatype->writer :int64 dst unchecked?) 0 src-buf)
            :float32 (.writeBlock (writer/datatype->writer :float32 dst unchecked?) 0 src-buf)
            :float64 (.writeBlock (writer/datatype->writer :float64 dst unchecked?) 0 src-buf))
          :else
          ;;Punt!!
          (if parallel?
            (parallel-write dst src n-elems unchecked-reads?)
            (serial-write dst src n-elems unchecked-reads?))))))
  dst)
