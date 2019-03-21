(ns tech.datatype.io
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting
             :refer [numeric-type? integer-type? numeric-byte-width
                     datatype->host-type]
             :as casting]
            [tech.jna :as jna]
            [tech.parallel :as parallel]
            [clojure.set :as c-set]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.datatype.fast-copy :as fast-copy]
            [tech.datatype.typecast :as typecast]
            [clojure.core.matrix.protocols :as mp])

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





(defmacro datatype->parallel-write
  [datatype dst src n-elems unchecked?]
  `(let [writer# (typecast/datatype->writer ~datatype ~dst true)
         reader# (typecast/datatype->reader ~datatype ~src ~unchecked?)]
     (parallel/parallel-for
      idx#
      ~n-elems
      (.write writer# idx# (.read reader# idx#)))))


(defmacro datatype->serial-write
  [datatype dst src n-elems unchecked?]
  `(let [writer# (typecast/datatype->writer ~datatype ~dst true)
         reader# (typecast/datatype->reader ~datatype ~src ~unchecked?)
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
  [dst src unchecked? parallel?]
  (let [dst-dtype (dtype-proto/get-datatype dst)
        src-dtype (dtype-proto/get-datatype src)
        src-buf (if (casting/numeric-type? src-dtype)
                  (typecast/as-nio-buffer src)
                  (typecast/as-list src))
        dst-buf (if (casting/numeric-type? dst-dtype)
                  (typecast/as-nio-buffer dst)
                  (typecast/as-list dst))
        src-buf-dtype (when src-buf (dtype-proto/get-datatype src-buf))
        dst-buf-dtype (when dst-buf (dtype-proto/get-datatype dst-buf))
        fast-path? (and src-buf
                        dst-buf
                        (or (and (numeric-type? dst-dtype)
                                 (= dst-dtype src-dtype)
                                 (= dst-buf-dtype dst-dtype))
                            (and (integer-type? dst-dtype)
                                 (integer-type? src-dtype)
                                 (= (casting/datatype->host-datatype dst-dtype) dst-buf-dtype)
                                 (= (casting/datatype->host-datatype src-dtype) src-buf-dtype)
                                 unchecked?
                                 (= (numeric-byte-width dst-dtype)
                                    (numeric-byte-width src-dtype)))))]
    ;;Fast path means no conversion is necessary and we can hit optimized
    ;;bulk pathways
    (if fast-path?
      ;;The only real special case is if one side is a ptr
      ;;and the other is an array
      (fast-copy/copy! dst src)
      ;;The slow path still has fast aspects if one of the buffers is a known primitive type.
      (let [unchecked-reads? (or unchecked?
                                 (= dst-dtype src-dtype))]
        (cond
          (and dst-buf
               (or (= dst-dtype dst-buf-dtype)
                   (and unchecked?
                        (= (casting/datatype->safe-host-type dst-dtype)
                           dst-buf-dtype))))
          (case dst-dtype
            :int8 (.readBlock (typecast/datatype->reader :int8 src unchecked?) 0 dst-buf)
            :uint8 (.readBlock (typecast/datatype->reader :uint8 src unchecked?) 0 dst-buf)
            :int16 (.readBlock (typecast/datatype->reader :int16 src unchecked?) 0 dst-buf)
            :uint16 (.readBlock (typecast/datatype->reader :uint16 src unchecked?) 0 dst-buf)
            :int32 (.readBlock (typecast/datatype->reader :int32 src unchecked?) 0 dst-buf)
            :uint32 (.readBlock (typecast/datatype->reader :uint32 src unchecked?) 0 dst-buf)
            :int64 (.readBlock (typecast/datatype->reader :int64 src unchecked?) 0 dst-buf)
            :uint64 (.readBlock (typecast/datatype->reader :uint64 src unchecked?) 0 dst-buf)
            :float32 (.readBlock (typecast/datatype->reader :float32 src unchecked?) 0 dst-buf)
            :float64 (.readBlock (typecast/datatype->reader :float64 src unchecked?) 0 dst-buf)
            :boolean (.readBlock (typecast/datatype->reader :boolean src unchecked?) 0 dst-buf)
            :object (.readBlock (typecast/datatype->reader :object src unchecked?) 0 dst-buf)
            )

          ;;Src has nio buffer and nio buffer matches datatype or src
          (and src-buf
               (or (= src-dtype src-buf-dtype)
                   (and unchecked?
                        (= (casting/datatype->safe-host-type src-dtype)
                           (dtype-proto/get-datatype src-buf)))))
          (case (dtype-proto/get-datatype src-buf)
            :int8 (.writeBlock (typecast/datatype->writer :int8 dst unchecked?) 0 src-buf)
            :uint8  (.writeBlock (typecast/datatype->writer :uint8 dst unchecked?) 0 src-buf)
            :int16 (.writeBlock (typecast/datatype->writer :int16 dst unchecked?) 0 src-buf)
            :uint16 (.writeBlock (typecast/datatype->writer :uint16 dst unchecked?) 0 src-buf)
            :int32 (.writeBlock (typecast/datatype->writer :int32 dst unchecked?) 0 src-buf)
            :uint32 (.writeBlock (typecast/datatype->writer :uint32 dst unchecked?) 0 src-buf)
            :int64 (.writeBlock (typecast/datatype->writer :int64 dst unchecked?) 0 src-buf)
            :uint64 (.writeBlock (typecast/datatype->writer :uint64 dst unchecked?) 0 src-buf)
            :float32 (.writeBlock (typecast/datatype->writer :float32 dst unchecked?) 0 src-buf)
            :float64 (.writeBlock (typecast/datatype->writer :float64 dst unchecked?) 0 src-buf)
            :boolean (.writeBlock (typecast/datatype->writer :boolean dst unchecked?) 0 src-buf)
            :object (.writeBlock (typecast/datatype->writer :object dst unchecked?) 0 src-buf))
          :else
          ;;Punt!!
          (if parallel?
            (parallel-write dst src (mp/element-count dst) unchecked-reads?)
            (serial-write dst src (mp/element-count dst) unchecked-reads?))))))
  dst)


(defn make-transfer-buffer
  "Easy marshalling means being able to make a transfer buffer"
  [src-buffer intermediate-dtype result-dtype array-required? unchecked?]
  (when-not (= result-dtype
               (casting/datatype->host-datatype result-dtype))
    (throw (ex-info "Destination type must be a native jvm datatype"
                    {:dst-dtype result-dtype})))
  (let [src-dtype (dtype-proto/get-datatype src-buffer)
        n-elems (int (mp/element-count src-buffer))
        dst-buffer (if (or (and (= src-dtype result-dtype)
                                (= src-dtype intermediate-dtype))
                           (and unchecked?
                                (= (casting/datatype->host-datatype src-dtype)
                                   (casting/datatype->host-datatype result-dtype))
                                (= (casting/datatype->host-datatype src-dtype)
                                   (casting/datatype->host-datatype intermediate-dtype))))
                     src-buffer
                     (let [dst-buffer (typecast/make-interface-buffer-type result-dtype n-elems)
                           dst-copy-buf (if (= result-dtype intermediate-dtype)
                                          dst-buffer
                                          (dtype-proto/->typed-buffer dst-buffer intermediate-dtype))]
                       (dense-copy! dst-copy-buf
                                    src-buffer unchecked? true)
                       dst-buffer))]
    ;;If we are dealing with a native-backed nio buffer
    (if (and array-required?
             (casting/numeric-type? result-dtype)
             (not (dtype-proto/->sub-array dst-buffer)))
      (dense-copy! (typecast/make-interface-buffer-type result-dtype n-elems)
                   dst-buffer true true)
      dst-buffer)))
