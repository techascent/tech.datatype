(ns tech.datatype.io
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting
             :refer [numeric-type? integer-type? numeric-byte-width
                     datatype->jvm-type]
             :as casting]
            [tech.jna :as jna]
            [tech.parallel :as parallel]
            [clojure.set :as c-set]
            [clojure.core.matrix.macros :refer [c-for]])

  (:import [tech.datatype
            ObjectReader ObjectWriter Mutable
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
  ObjectReader
  (get-datatype [item] :object)
  ObjectWriter
  (get-datatype [item] :object)
  Mutable
  (get-datatype [item] :object)
  ByteReader
  (get-datatype [item] :int8)
  ByteWriter
  (get-datatype [item] :int8)
  ByteMutable
  (get-datatype [item] :int8)
  ShortReader
  (get-datatype [item] :int16)
  ShortWriter
  (get-datatype [item] :int16)
  ShortMutable
  (get-datatype [item] :int16)
  IntReader
  (get-datatype [item] :int32)
  IntWriter
  (get-datatype [item] :int32)
  IntMutable
  (get-datatype [item] :int32)
  LongReader
  (get-datatype [item] :int64)
  LongWriter
  (get-datatype [item] :int64)
  LongMutable
  (get-datatype [item] :int64)
  FloatReader
  (get-datatype [item] :float32)
  FloatWriter
  (get-datatype [item] :float32)
  FloatMutable
  (get-datatype [item] :float32)
  DoubleReader
  (get-datatype [item] :float64)
  DoubleWriter
  (get-datatype [item] :float64)
  DoubleMutable
  (get-datatype [item] :float64)
  BooleanReader
  (get-datatype [item] :boolean)
  BooleanWriter
  (get-datatype [item] :boolean)
  BooleanMutable
  (get-datatype [item] :boolean))


(defn ->object-reader ^ObjectReader [item]
  (if (instance? ObjectReader item)
    item
    (dtype-proto/->object-reader item)))

(extend-type ObjectReader
  dtype-proto/PToReader
  (->object-reader [item] item)
  (->reader-of-type [item datatype] (throw (ex-info "unimplemented" {}))))

(defn ->object-writer ^ObjectWriter [item]
  (if (instance? ObjectWriter item)
    item
    (dtype-proto/->object-writer item)))

(extend-type ObjectWriter
  dtype-proto/PToWriter
  (->object-writer [item] item)
  (->writer-of-type [item datatype] (throw (ex-info "unimplemented" {}))))

(defn ->object-mutable ^Mutable [item]
  (if (instance? Mutable item)
    item
    (dtype-proto/->object-mutable item)))


(defn ->byte-reader ^ByteReader [item unchecked?]
  (if (instance? ByteReader item)
    item
    (dtype-proto/->reader-of-type item :int8 unchecked?)))
(defn ->byte-writer ^ByteWriter [item]
  (if (instance? ByteWriter item)
    item
    (dtype-proto/->writer-of-type item :int8)))
(defn ->byte-mutable ^ByteMutable [item]
  (if (instance? ByteMutable item)
    item
    (dtype-proto/->mutable-of-type item :int8)))


(defn ->short-reader ^ShortReader [item unchecked?]
  (if (instance? ShortReader item)
    item
    (dtype-proto/->reader-of-type item :int16 unchecked?)))
(defn ->short-writer ^ShortWriter [item]
  (if (instance? ShortWriter item)
    item
    (dtype-proto/->writer-of-type item :int16)))
(defn ->short-mutable ^ShortMutable [item]
  (if (instance? ShortMutable item)
    item
    (dtype-proto/->mutable-of-type item :int16)))


(defn ->int-reader ^IntReader [item unchecked?]
  (if (instance? IntReader item)
    item
    (dtype-proto/->reader-of-type item :int32 unchecked?)))
(defn ->int-writer ^IntWriter [item]
  (if (instance? IntWriter item)
    item
    (dtype-proto/->writer-of-type item :int32)))
(defn ->int-mutable ^IntMutable [item]
  (if (instance? IntMutable item)
    item
    (dtype-proto/->mutable-of-type item :int32)))


(defn ->long-reader ^LongReader [item unchecked?]
  (if (instance? LongReader)
    item
    (dtype-proto/->reader-of-type item :int64 unchecked?)))
(defn ->long-writer ^LongWriter [item]
  (if (instance? LongWriter)
    item
    (dtype-proto/->writer-of-type item :int64)))
(defn ->long-mutable ^LongMutable [item]
  (if (instance? LongMutable item)
    item
    (dtype-proto/->mutable-of-type item :int64)))


(defn ->float-reader ^FloatReader [item unchecked?]
  (if (instance? FloatReader item)
    item
    (dtype-proto/->reader-of-type item :float32 unchecked?)))
(defn ->float-writer ^FloatWriter [item]
  (if (instance? FloatWriter item)
    item
    (dtype-proto/->writer-of-type item :float32)))
(defn ->float-mutable ^FloatMutable [item]
  (if (instance? FloatMutable item)
    item
    (dtype-proto/->mutable-of-type item :float32)))


(defn ->double-reader ^DoubleReader [item unchecked?]
  (if (instance? DoubleReader item)
    item
    (dtype-proto/->reader-of-type item :float64 unchecked?)))
(defn ->double-writer ^DoubleWriter [item]
  (if (instance? DoubleWriter item)
    item
    (dtype-proto/->writer-of-type item :float64)))
(defn ->double-mutable ^DoubleMutable [item]
  (if (instance? DoubleMutable item)
    item
    (dtype-proto/->mutable-of-type item :float64)))


(defn ->boolean-reader ^BooleanReader [item unchecked?]
  (if (instance? BooleanReader item)
    item
    (dtype-proto/->reader-of-type item :boolean unchecked?)))
(defn ->boolean-writer ^BooleanWriter [item]
  (if (instance? BooleanWriter item)
    item
    (dtype-proto/->writer-of-type item :boolean)))
(defn ->boolean-mutable ^BooleanMutable [item]
  (if (instance? BooleanMutable item)
    item
    (dtype-proto/->mutable-of-type item :boolean)))

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


(defmacro datatype->writer
  [datatype item]
  (case datatype
    :int8 `(->byte-writer ~item)
    :uint8 `(->byte-writer ~item)
    :int16 `(->short-writer ~item)
    :uint16 `(->short-writer ~item)
    :int32 `(->int-writer ~item)
    :uint32 `(->int-writer ~item)
    :int64 `(->long-writer ~item)
    :uint64 `(->long-writer ~item)
    :float32 `(->float-writer ~item)
    :float64 `(->double-writer ~item)
    :boolean `(->boolean-writer ~item)
    `(->object-writer ~item)))


(defmacro datatype->reader
  [datatype item unchecked?]
  (case datatype
    :int8 `(->byte-reader ~item ~unchecked?)
    :uint8 `(->byte-reader ~item ~unchecked?)
    :int16 `(->short-reader ~item ~unchecked?)
    :uint16 `(->short-reader ~item ~unchecked?)
    :int32 `(->int-reader ~item ~unchecked?)
    :uint32 `(->int-reader ~item ~unchecked?)
    :int64 `(->long-reader ~item ~unchecked?)
    :uint64 `(->long-reader ~item ~unchecked?)
    :float32 `(->float-reader ~item ~unchecked?)
    :float64 `(->double-reader ~item ~unchecked?)
    :boolean `(->boolean-reader ~item ~unchecked?)
    `(->object-reader ~item)))


(defmacro with-typed-writer
  [item & body]
  `(case (dtype-proto/get-datatype ~item)
     :int8 (let [~'writer (datatype->writer :int8 ~item)] ~@body)
     :int16 (let [~'writer (datatype->writer :int16 ~item)] ~@body)
     :int32 (let [~'writer (datatype->writer :int32 ~item)] ~@body)
     :int64 (let [~'writer (datatype->writer :int64 ~item)] ~@body)
     :float32 (let [~'writer (datatype->writer :float32 ~item)] ~@body)
     :float64 (let [~'writer (datatype->writer :float64 ~item)] ~@body)
     :boolean (let [~'writer (datatype->writer :boolean ~item)] ~@body)
     (let [~'writer (datatype->writer :object ~item)] ~@body)))


(defmacro with-typed-reader
  [item & body]
  `(case (dtype-proto/get-datatype ~item)
     :int8 (let [~'reader (datatype->reader :int8 ~item true)] ~@body)
     :int16 (let [~'reader (datatype->reader :int16 ~item true)] ~@body)
     :int32 (let [~'reader (datatype->reader :int32 ~item true)] ~@body)
     :int64 (let [~'reader (datatype->reader :int64 ~item true)] ~@body)
     :float32 (let [~'reader (datatype->reader :float32 ~item true)] ~@body)
     :float64 (let [~'reader (datatype->reader :float64 ~item true)] ~@body)
     :boolean (let [~'reader (datatype->reader :boolean ~item true)] ~@body)
     (let [~'reader (datatype->reader :object ~item false)] ~@body)))


(defn make-object-writer
  [item datatype]
  (when-not (casting/is-host-datatype? (dtype-proto/get-datatype item))
    (throw (ex-info "Must make writers from containers of host types."
                    {:datatype datatype})))
  (with-typed-writer item
    (reify ObjectWriter
        (write [item idx value]
          (.write writer idx (casting/jvm-cast value datatype)))
        (writeConstant [item idx value n-elems]
          (.writeConstant writer idx (casting/jvm-cast value datatype)
                          n-elems))
        (writeBlock [item-writer offset values]
          (doseq [[idx val] (map-indexed vector (seq values))]
            (.write item-writer (+ offset (long idx)) val)))
        (writeIndexes [item-writer indexes values]
          (doseq [[idx val] (map vector
                                 (dtype-proto/->vector indexes)
                                 (seq values))]
            (.write item-writer (long idx) val))))))


(defn make-object-reader
  [src-container datatype]
  (when-not (casting/is-host-datatype? (dtype-proto/get-datatype src-container))
    (throw (ex-info "Must make readers from containers of host types."
                    {:datatype datatype})))
  (with-typed-reader src-container
    (reify ObjectReader
      (read [item# idx]
        (casting/unchecked-cast
         (.read reader idx)
         datatype))
      (readBlock [reader-item offset dest]
        (let [dest-size (.size dest)]
          (c-for [idx (int 0) (< idx dest-size) (inc idx)]
                 (.set dest idx
                       (.read reader-item (+ offset idx)))))
        dest)
      (readIndexes [reader-item indexes dest]
        (let [dest-size (.size dest)]
          (c-for [idx (int 0) (< idx dest-size) (inc idx)]
                 (.set dest idx
                       (.read reader-item
                              (.get indexes
                                    (+ (.position indexes)
                                       idx)))))
          dest)))))


(defmacro datatype->parallel-write
  [datatype dst src n-elems unchecked?]
  `(let [writer# (datatype->writer ~datatype ~dst)
         reader# (datatype->reader ~datatype ~src ~unchecked?)]
     (parallel/parallel-for
      idx#
      ~n-elems
      (.write writer# idx# (.read reader# idx#)))))


(defmacro datatype->serial-write
  [datatype dst src n-elems unchecked?]
  `(let [writer# (datatype->writer ~datatype ~dst)
         reader# (datatype->reader ~datatype ~src ~unchecked?)
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
      (let [unchecked-reads? (or unchecked?
                                 (= dst-dtype src-dtype))]
        (if parallel?
          (parallel-write dst src n-elems unchecked-reads?)
          (serial-write dst src n-elems unchecked-reads?)))))
  dst)
