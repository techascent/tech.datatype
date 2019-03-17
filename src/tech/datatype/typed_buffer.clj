(ns tech.datatype.typed-buffer
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting :as casting]
            [tech.datatype.io :as dtype-io]
            [tech.datatype.base :as base]
            [tech.parallel :as parallel]
            [tech.datatype.reader :as reader]
            [tech.jna :as jna]
            [clojure.core.matrix.macros :refer [c-for]]
            [clojure.core.matrix.protocols :as mp])
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



(defrecord TypedBuffer [datatype backing-store]
  dtype-proto/PDatatype
  (get-datatype [item] datatype)

  dtype-proto/PContainerType
  (container-type [item] :typed-buffer)
  (dense-container? [item] true)
  (sparse-container? [item] false)

  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (base/raw-dtype-copy! raw-data ary-target target-offset options))

  dtype-proto/PPersistentVector
  (->vector [item]
    (vec (dtype-proto/->array-copy item)))


  dtype-proto/PPrototype
  (from-prototype [item datatype shape]
    (->TypedBuffer datatype
                   (dtype-proto/from-prototype backing-store
                                               (casting/datatype->jvm-type datatype)
                                               shape)))
  dtype-proto/PToNioBuffer
  (->buffer-backing-store [item]
    (dtype-proto/->buffer-backing-store backing-store))

  dtype-proto/PToTypedBuffer
  (->typed-buffer [item] item)
  (->typed-sparse-buffer [item] nil)


  dtype-proto/PBuffer
  (sub-buffer [buffer offset length]
    (->TypedBuffer datatype (dtype-proto/sub-buffer backing-store offset length)))
  (alias? [lhs-buffer rhs-buffer]
    (when-let [rhs-nio (dtype-io/as-nio-buffer rhs-buffer)]
      (dtype-proto/alias? backing-store rhs-nio)))
  (partially-alias? [lhs-buffer rhs-buffer]
    (when-let [rhs-nio (dtype-io/as-nio-buffer rhs-buffer)]
      (dtype-proto/alias? backing-store rhs-nio)))


  dtype-proto/PToArray
  (->array [item]
    (when (= datatype (dtype-proto/get-datatype backing-store))
      (dtype-proto/->array backing-store)))
  (->sub-array [item]
    (when (= datatype (dtype-proto/get-datatype backing-store))
      (dtype-proto/->sub-array backing-store)))
  (->array-copy [item]
    (if (= datatype (dtype-proto/get-datatype backing-store))
      (dtype-proto/->array-copy backing-store)
      (let [data-buf (dtype-proto/make-container
                      :jvm-array  (casting/datatype->safe-jvm-type datatype)
                      (base/ecount backing-store))]
        (base/copy! item 0 data-buf 0 (base/ecount item)))))

  dtype-proto/PToWriter
  (->object-writer [item]
    (dtype-io/make-object-writer backing-store datatype))
  ;;No marshalling/casting on the writer side.
  (->writer-of-type [item writer-datatype]
    (when (= datatype (dtype-proto/get-datatype backing-store))
      (dtype-proto/->writer-of-type backing-store writer-datatype)))

  dtype-proto/PToReader
  (->object-reader [item]
    (dtype-io/make-object-reader backing-store datatype))

  (->reader-of-type [item reader-datatype unchecked?]
    ;;Make a reader that reads to our datatype first
    (let [temp-reader (reader/make-reader backing-store datatype true)]
      (if-not (= datatype reader-datatype)
        ;;now make a reader that does outward conversion to dest datatype.
        (case (dtype-proto/get-datatype backing-store)
          :int8 (reader/make-marshalling-reader (dtype-io/datatype->reader :int8 temp-reader)
                                                :int8
                                                reader-datatype unchecked?)
          :int16 (reader/make-marshalling-reader (dtype-io/datatype->reader :int16 temp-reader)
                                                 :int16
                                                 reader-datatype unchecked?)
          :int32 (reader/make-marshalling-reader (dtype-io/datatype->reader :int32 temp-reader)
                                                 :int32
                                                 reader-datatype unchecked?)
          :int64 (reader/make-marshalling-reader (dtype-io/datatype->reader :int64 temp-reader)
                                                 :int64
                                                 reader-datatype unchecked?)
          :float32 (reader/make-marshalling-reader (dtype-io/datatype->reader :float32 temp-reader)
                                                 :float32
                                                reader-datatype unchecked?)
          :float64 (reader/make-marshalling-reader (dtype-io/datatype->reader :float64 temp-reader)
                                                 :float64
                                                 reader-datatype unchecked?)))))

  mp/PElementCount
  (element-count [item] (mp/element-count backing-store)))

(defn typed-buffer-like?
  [item]
  (every? #(satisfies? % item)
          [dtype-proto/PDatatype
           dtype-proto/PContainerType dtype-proto/PCopyRawData
           dtype-proto/PPersistentVector dtype-proto/PPrototype
           dtype-proto/PToNioBuffer dtype-proto/PToTypedBuffer
           dtype-proto/PBuffer dtype-proto/PToArray
           dtype-proto/PToWriter dtype-proto/PToReader]))


(defn convertible-to-typed-buffer?
  [item]
  (or (instance? TypedBuffer item)
      (satisfies? dtype-proto/PToNioBuffer item)))


(defn ->typed-buffer
  [item]
  (cond
    (instance? TypedBuffer item)
    item
    (satisfies? dtype-proto/PToNioBuffer item)
    (->TypedBuffer (dtype-proto/get-datatype item) (dtype-proto/->buffer-backing-store item))
    :else
    (throw (ex-info "Item is not convertible to typed buffer"
                    {:item-type (type item)}))))
