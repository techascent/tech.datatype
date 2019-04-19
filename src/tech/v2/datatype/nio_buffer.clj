(ns tech.v2.datatype.nio-buffer
  "Nio buffers really are the workhorses of the entire system."
  (:require [tech.jna :as jna]
            [tech.v2.datatype.io :as dtype-io]
            [tech.v2.datatype.base :as base]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :refer :all :as typecast]
            [tech.v2.datatype.nio-access
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
            [tech.v2.datatype.reader :as reader]
            [tech.v2.datatype.writer :as writer]
            [tech.v2.datatype.fast-copy :as fast-copy]
            [tech.jna :as jna]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.parallel :as parallel]
            [tech.v2.datatype.array])
  (:import [com.sun.jna Pointer Native]
           [java.nio Buffer ByteBuffer ShortBuffer
            IntBuffer LongBuffer FloatBuffer DoubleBuffer]
           [tech.v2.datatype
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
  dtype-proto/PToIterable
  (->iterable-of-type [item datatype unchecked?]
    (dtype-proto/->reader-of-type item datatype unchecked?))
  mp/PElementCount
  (element-count [item] (.remaining item)))


(declare make-buffer-of-type)


(defn in-range?
  [^long lhs-off ^long lhs-len ^long rhs-off ^long rhs-len]
    (or (and (>= rhs-off lhs-off)
           (< rhs-off (+ lhs-off lhs-len)))
      (and (>= lhs-off rhs-off)
           (< lhs-off (+ rhs-off rhs-len)))))


(jna/def-jna-fn (jna/c-library-name) memset
  "Set a block of memory to a value"
  Pointer
  [data ensure-ptr-like]
  [val int]
  [num-bytes int])



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
                        (let [n-elems# (base/shape->ecount shape#)]
                          (if (.isDirect (datatype->buffer-cast-fn ~datatype src-ary#))
                            (dtype-proto/make-container :native-buffer datatype#
                                                        n-elems# {})
                            (dtype-proto/make-container :nio-buffer datatype#
                                                        n-elems# {}))))}

     dtype-proto/PToBackingStore
     {:->backing-store-seq (fn [item#]
                             (if-let [data-ary# (dtype-proto/->sub-array item#)]
                               [data-ary#]
                               [(jna/->ptr-backing-store item#)]))}

     dtype-proto/PToNioBuffer
     {:convertible-to-nio-buffer? (fn [item#] true)
      :->buffer-backing-store (fn [item#] item#)}

     dtype-proto/PToArray
     {:->sub-array (fn [item#]
                     (let [item# (datatype->buffer-cast-fn ~datatype item#)]
                       (when-not (.isDirect item#)
                         {:java-array (.array item#)
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

     dtype-proto/PSetConstant
     {:set-constant!
      (fn [item# offset# value# elem-count#]
        (let [item# (datatype->buffer-cast-fn ~datatype item#)
              offset# (int offset#)
              elem-count# (int elem-count#)
              value# (casting/cast value# ~datatype)
              zero-val# (casting/cast 0 ~datatype)]
          (if (or (= value# zero-val#)
                  (= ~datatype :int8))
            (memset (dtype-proto/sub-buffer item# offset# elem-count#)
                    (int value#)
                    (* elem-count# (casting/numeric-byte-width ~datatype)))
            (let [buf-pos# (.position item#)]
              (parallel/parallel-for
               idx# elem-count#
               (buf-put item# (+ idx# offset#) buf-pos# value#))))))}


     dtype-proto/PBuffer
     {:sub-buffer (fn [buffer# offset# length#]
                    (let [buf# (.slice (datatype->buffer-cast-fn ~datatype buffer#))
                          offset# (long offset#)
                          len# (long length#)]
                      (.position buf# offset#)
                      (.limit buf# (+ offset# len#))
                      buf#))}
     dtype-proto/PToWriter
     {:->writer-of-type
      (fn [item# writer-datatype# unchecked?#]
        (if (= writer-datatype# ~datatype)
          (writer/make-buffer-writer item# unchecked?#)
          (-> (writer/make-buffer-writer item# true)
              (dtype-proto/->writer-of-type writer-datatype# unchecked?#))))}


     dtype-proto/PToReader
     {:->reader-of-type
      (fn [item# reader-datatype# unchecked?#]
        (cond-> (reader/make-buffer-reader item#)
          (not= reader-datatype# ~datatype)
          (dtype-proto/->reader-of-type reader-datatype# unchecked?#)))}


     jna/PToPtr
     {:is-jna-ptr-convertible?
      (fn [item#]
        (let [item# (datatype->buffer-cast-fn ~datatype item#)]
          (.isDirect item#)))
      :->ptr-backing-store
      (fn [item#]
        (let [item# (datatype->buffer-cast-fn ~datatype item#)]
          (when (.isDirect item#)
            (let [ptr-addr# (Pointer/nativeValue (Native/getDirectBufferPointer item#))]
              (Pointer. (+ ptr-addr# (* (.position item#)
                                        (casting/numeric-byte-width ~datatype))))))))}))


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
