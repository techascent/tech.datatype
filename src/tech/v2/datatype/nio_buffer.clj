(ns tech.v2.datatype.nio-buffer
  "Nio buffers really are the workhorses of the entire system."
  (:require [tech.jna :as jna]
            [tech.v2.datatype.base :as base]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.nio-access
             :refer [buf-put]]
            [tech.v2.datatype.reader :as reader]
            [tech.v2.datatype.writer :as writer]
            [tech.v2.datatype.array]
            [tech.resource :as resource]
            [tech.parallel.for :as parallel-for])
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
  (->iterable-of-type [item options]
    (dtype-proto/->reader item options)))


(declare make-buffer-of-type)


(defmacro datatype->buffer-creation
  [datatype src-ary]
  (case datatype
    :int8 `(ByteBuffer/wrap ^bytes ~src-ary)
    :int16 `(ShortBuffer/wrap ^shorts ~src-ary)
    :int32 `(IntBuffer/wrap ^ints ~src-ary)
    :int64 `(LongBuffer/wrap ^longs ~src-ary)
    :float32 `(FloatBuffer/wrap ^floats ~src-ary)
    :float64 `(DoubleBuffer/wrap ^doubles ~src-ary)))


(defn in-range?
  [^long lhs-off ^long lhs-len ^long rhs-off ^long rhs-len]
    (or (and (>= rhs-off lhs-off)
           (< rhs-off (+ lhs-off lhs-len)))
      (and (>= lhs-off rhs-off)
           (< lhs-off (+ rhs-off rhs-len)))))


(jna/def-jna-fn (jna/c-library-name) memset
  "Set a block of memory to a value"
  Pointer
  [data typecast/ensure-ptr-like]
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
                          (if (.isDirect (typecast/datatype->buffer-cast-fn
                                          ~datatype src-ary#))
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
                     (let [item# (typecast/datatype->buffer-cast-fn ~datatype item#)]
                       (when-not (.isDirect item#)
                         {:java-array (.array item#)
                          :offset (.position item#)
                          :length (dtype-proto/ecount item#)})))
      :->array-copy (fn [item#]
                      (let [dst-ary# (base/make-container
                                      :java-array ~datatype
                                      (dtype-proto/ecount item#))]
                        (base/copy! item# dst-ary#)))}
     dtype-proto/PNioBuffer
     {:position (fn [item#] (.position (typecast/datatype->buffer-cast-fn
                                        ~datatype item#)))
      :limit (fn [item#] (.limit (typecast/datatype->buffer-cast-fn ~datatype item#)))
      :array-backed? (fn [item#] (not (.isDirect (typecast/datatype->buffer-cast-fn
                                                  ~datatype item#))))}

     dtype-proto/PSetConstant
     {:set-constant!
      (fn [item# offset# value# elem-count#]
        (let [item# (typecast/datatype->buffer-cast-fn ~datatype item#)
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
              (parallel-for/parallel-for
               idx# elem-count#
               (buf-put item# (+ idx# offset#) buf-pos# value#))))))}


     dtype-proto/PBuffer
     {:sub-buffer
      (fn [buffer# offset# length#]
        (let [buffer# (typecast/datatype->buffer-cast-fn
                       ~datatype buffer#)
              sub-array-data# (dtype-proto/->sub-array buffer#)]
          (if sub-array-data#
            (let [{java-array# :java-array
                   ary-offset# :offset} sub-array-data#
                  buf# (datatype->buffer-creation ~datatype java-array#)
                  new-offset# (+ (long offset#) (long ary-offset#))]
              (.position buf# new-offset#)
              (.limit buf# (+ new-offset# (long length#)))
              buf#)
            (let [buf# (.slice buffer#)
                  offset# (long offset#)
                  len# (long length#)]
              (.position buf# offset#)
              (.limit buf# (+ offset# len#))
              buf#))))}
     dtype-proto/PToWriter
     {:convertible-to-writer? (constantly true)
      :->writer
      (fn [item# options#]
        (let [{writer-datatype# :datatype
               unchecked?# :unchecked?} options#]
          (-> (writer/make-buffer-writer item#
                                         ~(casting/safe-flatten datatype)
                                         ~datatype
                                         unchecked?#)
              (dtype-proto/->writer options#))))}


     dtype-proto/PToReader
     {:convertible-to-reader? (constantly true)
      :->reader
      (fn [item# options#]
        (let [{reader-datatype# :datatype
               unchecked?# :unchecked?} options#]
          (-> (reader/make-buffer-reader item#
                                         ~(casting/safe-flatten datatype)
                                         ~datatype
                                         unchecked?#)
              (dtype-proto/->reader options#))))}


     dtype-proto/PToIterable
     {:convertible-to-iterable? (constantly true)
      :->iterable (fn [item# options#] (dtype-proto/->reader item# options#))}


     jna/PToPtr
     {:is-jna-ptr-convertible?
      (fn [item#]
        (let [item# (typecast/datatype->buffer-cast-fn ~datatype item#)]
          (.isDirect item#)))
      :->ptr-backing-store
      (fn [item#]
        (let [item# (typecast/datatype->buffer-cast-fn ~datatype item#)]
          (when (.isDirect item#)
            (let [ptr-addr# (Pointer/nativeValue (Native/getDirectBufferPointer item#))
                  retval#
                  (Pointer. (+ ptr-addr#
                               (* (.position item#)
                                  (casting/numeric-byte-width ~datatype))))
                  resource-map# {:item item#}]
              ;;The resource system is used to make sure the gc can track the linkage
              ;;between the offset pointer and the source buffer.
              (resource/track retval# #(get resource-map# :item) [:gc])))))}
     dtype-proto/PToBufferDesc
     {:convertible-to-buffer-desc? (fn [item#] (jna/is-jna-ptr-convertible? item#))
      :->buffer-descriptor
      (fn [item#]
        (when-let [buf-ptr# (jna/->ptr-backing-store item#)]
          {:ptr buf-ptr#
           :datatype ~datatype
           :shape [(base/ecount item#)]
           :strides [(casting/numeric-byte-width ~datatype)]}))}))


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
  [_container-type datatype elem-count-or-seq options]
  (make-buffer-of-type datatype elem-count-or-seq options))
