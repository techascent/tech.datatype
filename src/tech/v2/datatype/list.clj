(ns tech.v2.datatype.list
  (:require [tech.v2.datatype.base :as base]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.array :as dtype-array]
            [tech.v2.datatype.nio-buffer :as nio-buffer]
            [tech.v2.datatype.typed-buffer :as typed-buffer]
            [tech.v2.datatype.reader :refer [make-buffer-reader] :as reader]
            [tech.v2.datatype.writer :refer [make-buffer-writer] :as writer]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.nio-access :refer [buf-put buf-get
                                              datatype->pos-fn
                                              datatype->read-fn
                                              datatype->write-fn
                                              datatype->list-read-fn]]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.mutable :as mutable]
            [tech.v2.datatype.mutable.iterable-to-list :as iterable-to-list]
            [tech.parallel.for :as parallel-for])
  (:import [it.unimi.dsi.fastutil.bytes ByteList ByteArrayList]
           [it.unimi.dsi.fastutil.shorts ShortList ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntList IntArrayList]
           [it.unimi.dsi.fastutil.longs LongList LongArrayList]
           [it.unimi.dsi.fastutil.floats FloatList FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleList DoubleArrayList]
           [it.unimi.dsi.fastutil.booleans BooleanList BooleanArrayList]
           [it.unimi.dsi.fastutil.objects ObjectList ObjectArrayList]
           [java.nio ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer Buffer]
           [java.util List ArrayList Arrays RandomAccess]
           [tech.v2.datatype
            ObjectReader ObjectWriter ObjectMutable
            ByteReader ByteWriter ByteMutable
            ShortReader ShortWriter ShortMutable
            IntReader IntWriter IntMutable
            LongReader LongWriter LongMutable
            FloatReader FloatWriter FloatMutable
            DoubleReader DoubleWriter DoubleMutable
            BooleanReader BooleanWriter BooleanMutable]
           [tech.v2.datatype.typed_buffer TypedBuffer]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)



(declare make-list)


(defn byte-array-list-cast ^ByteArrayList [item] item)
(defn short-array-list-cast ^ShortArrayList [item] item)
(defn int-array-list-cast ^IntArrayList [item] item)
(defn long-array-list-cast ^LongArrayList [item] item)
(defn float-array-list-cast ^FloatArrayList [item] item)
(defn double-array-list-cast ^DoubleArrayList [item] item)
(defn boolean-array-list-cast ^BooleanArrayList [item] item)
(defn object-array-list-cast ^ObjectArrayList [item] item)


(defmacro datatype->array-list-cast-fn
  [datatype item]
  (case datatype
    :int8 `(byte-array-list-cast ~item)
    :int16 `(short-array-list-cast ~item)
    :int32 `(int-array-list-cast ~item)
    :int64 `(long-array-list-cast ~item)
    :float32 `(float-array-list-cast ~item)
    :float64 `(double-array-list-cast ~item)
    :boolean `(boolean-array-list-cast ~item)
    :object `(object-array-list-cast ~item)))


(defn byte-list-cast ^ByteList [item] item)
(defn short-list-cast ^ShortList [item] item)
(defn int-list-cast ^IntList [item] item)
(defn long-list-cast ^LongList [item] item)
(defn float-list-cast ^FloatList [item] item)
(defn double-list-cast ^DoubleList [item] item)
(defn boolean-list-cast ^BooleanList [item] item)
(defn object-list-cast ^ObjectList [item] item)
(defn as-object-array ^"[Ljava.lang.Object;" [item] item)


(defmacro datatype->list-cast-fn
  [datatype item]
  (case datatype
    :int8 `(byte-list-cast ~item)
    :int16 `(short-list-cast ~item)
    :int32 `(int-list-cast ~item)
    :int64 `(long-list-cast ~item)
    :float32 `(float-list-cast ~item)
    :float64 `(double-list-cast ~item)
    :boolean `(boolean-list-cast ~item)
    :object `(object-list-cast ~item)))


(defmacro datatype->buffer-creation-length
  [datatype src-ary len]
  (case datatype
    :int8 `(ByteBuffer/wrap ^bytes ~src-ary 0 ~len)
    :int16 `(ShortBuffer/wrap ^shorts ~src-ary 0 ~len)
    :int32 `(IntBuffer/wrap ^ints ~src-ary 0 ~len)
    :int64 `(LongBuffer/wrap ^longs ~src-ary 0 ~len)
    :float32 `(FloatBuffer/wrap ^floats ~src-ary 0 ~len)
    :float64 `(DoubleBuffer/wrap ^doubles ~src-ary 0 ~len)))



(defmacro extend-list-type
  [typename datatype]
  `(clojure.core/extend
       ~typename
     dtype-proto/PDatatype
     {:get-datatype (fn [arg#] ~datatype)}


     dtype-proto/PPrototype
     {:from-prototype (fn [src-ary# datatype# shape#]
                        (when-not (= 1 (count shape#))
                          (throw (ex-info "Base containers cannot have complex shapes"
                                          {:shape shape#})))
                        (make-list datatype# (base/shape->ecount shape#)))}


     dtype-proto/PCountable
     {:ecount (fn [item#]
                (-> (datatype->list-cast-fn ~datatype item#)
                    (.size)))}

     dtype-proto/PToList
     {:convertible-to-fastutil-list? (fn [item#] true)
      :->list-backing-store (fn [item#] item#)}))


(extend-list-type ByteList :int8)
(extend-list-type ShortList :int16)
(extend-list-type IntList :int32)
(extend-list-type LongList :int64)
(extend-list-type FloatList :float32)
(extend-list-type DoubleList :float64)
(extend-list-type BooleanList :boolean)
(extend-list-type ObjectList :object)


(defmacro extend-numeric-list
  [typename datatype]
  `(clojure.core/extend
       ~typename
     dtype-proto/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (dtype-proto/copy-raw->item!
                          (dtype-proto/->buffer-backing-store raw-data#)
                          ary-target# target-offset# options#))}


     dtype-proto/PToBackingStore
     {:->backing-store-seq (fn [item#]
                             (-> (dtype-proto/->buffer-backing-store item#)
                                 (dtype-proto/->backing-store-seq)))}


     dtype-proto/PToNioBuffer
     {:convertible-to-nio-buffer? (fn [item#] true)
      :->buffer-backing-store (fn [item#]
                                (let [item# (datatype->array-list-cast-fn
                                             ~datatype item#)]
                                  (datatype->buffer-creation-length
                                   ~datatype (.elements item#) (.size item#))))}


     dtype-proto/PToWriter
    {:convertible-to-writer? (constantly true)
     :->writer
     (fn [item# options#]
       (-> (dtype-proto/as-nio-buffer item#)
           (dtype-proto/->writer options#)))}


    dtype-proto/PToReader
    {:convertible-to-reader? (constantly true)
     :->reader
     (fn [item# options#]
       (-> (dtype-proto/as-nio-buffer item#)
           (dtype-proto/->reader options#)))}


     dtype-proto/PToArray
     {:->sub-array (fn [item#]
                     (dtype-proto/->sub-array
                      (dtype-proto/->buffer-backing-store item#)))
      :->array-copy (fn [item#]
                      (dtype-proto/->array-copy
                       (dtype-proto/->buffer-backing-store item#)))}))


(extend-numeric-list ByteArrayList :int8)
(extend-numeric-list ShortArrayList :int16)
(extend-numeric-list IntArrayList :int32)
(extend-numeric-list LongArrayList :int64)
(extend-numeric-list FloatArrayList :float32)
(extend-numeric-list DoubleArrayList :float64)


(defn extend-array-list
  [typename]
  (clojure.core/extend
      typename
    dtype-proto/PToWriter
    {:convertible-to-writer? (constantly true)
     :->writer
     (fn [item options]
       (let [unchecked? (:unchecked? options)]
         (-> (writer/make-list-writer item
                                      (dtype-proto/get-datatype item)
                                      (dtype-proto/get-datatype item)
                                      unchecked?)
             (dtype-proto/->writer options))))}

    dtype-proto/PToReader
    {:convertible-to-reader? (constantly true)
     :->reader
     (fn [item options]
       (let [unchecked? (:unchecked? options)]
         (-> (reader/make-list-reader item
                                      (dtype-proto/get-datatype item)
                                      (dtype-proto/get-datatype item)
                                      unchecked?)
             (dtype-proto/->reader options))))}

    dtype-proto/PToIterable
    {:convertible-to-iterable? (constantly true)
     :->iterable (fn [item options] (dtype-proto/->reader item options))}

    dtype-proto/PToMutable
    {:convertible-to-mutable? (constantly true)
     :->mutable
     (fn [list-item options]
       (-> (mutable/make-list-mutable list-item
                                      (dtype-proto/get-datatype list-item)
                                      (dtype-proto/get-datatype list-item)
                                      true)
           (dtype-proto/->mutable options)))}))


(extend-array-list ByteArrayList)
(extend-array-list ShortArrayList)
(extend-array-list IntArrayList)
(extend-array-list LongArrayList)
(extend-array-list FloatArrayList)
(extend-array-list DoubleArrayList)
(extend-array-list BooleanArrayList)
(extend-array-list ObjectArrayList)


(defmacro datatype->as-array-list
  [datatype list-item]
  (case datatype
    :int8 `(byte-array-list-cast
            (when (instance? ByteArrayList ~list-item)
              ~list-item))
    :int16 `(short-array-list-cast
             (when (instance? ShortArrayList ~list-item)
               ~list-item))
    :int32 `(int-array-list-cast
             (when (instance? IntArrayList ~list-item)
               ~list-item))
    :int64 `(long-array-list-cast
             (when (instance? LongArrayList ~list-item)
               ~list-item))
    :float32 `(float-array-list-cast
               (when (instance? FloatArrayList ~list-item)
                 ~list-item))
    :float64 `(double-array-list-cast
               (when (instance? DoubleArrayList ~list-item)
                 ~list-item))
    :boolean `(boolean-array-list-cast
               (when (instance? BooleanArrayList ~list-item)
                 ~list-item))
    `(object-array-list-cast
      (when (instance? ObjectArrayList ~list-item)
        ~list-item))))



(defmacro extend-list
  [typename datatype]
  `(clojure.core/extend
       ~typename
     dtype-proto/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (base/raw-dtype-copy! raw-data# ary-target#
                                               target-offset# options#))}

     dtype-proto/PBuffer
     {:sub-buffer (fn [buffer# offset# length#]
                    (let [list-data# (datatype->list-cast-fn ~datatype buffer#)
                          offset# (int offset#)
                          length# (int length#)]
                      (.subList list-data# offset# (+ offset# length#))))}

     dtype-proto/PToArray
     {:->sub-array (fn [item#]
                     (when-let [ary-list# (datatype->as-array-list ~datatype item#)]
                       (let [dst-ary# (.elements ary-list#)]
                         {:java-array dst-ary#
                          :offset 0
                          :length (.size ary-list#)})))
      :->array-copy (fn [item#]
                      (.toArray (datatype->list-cast-fn ~datatype item#)
                                (typecast/datatype->array-cast-fn
                                 ~datatype
                                 (dtype-proto/make-container
                                  :java-array (casting/flatten-datatype
                                               (dtype-proto/get-datatype item#))
                                  0 {}))))}

     dtype-proto/PToWriter
     {:convertible-to-writer? (constantly true)
      :->writer
      (fn [item# options#]
        (let [unchecked?# (:unchecked? options#)]
          (-> (writer/make-list-writer item#
                                       (casting/safe-flatten ~datatype)
                                       ~datatype
                                       true)
              (dtype-proto/->writer options#))))}

     dtype-proto/PToReader
     {:convertible-to-reader? (constantly true)
      :->reader
      (fn [item# options#]
        (let [unchecked?# (:unchecked? options#)]
          (-> (reader/make-list-reader item#
                                       (casting/safe-flatten ~datatype)
                                       ~datatype
                                       true)
              (dtype-proto/->reader options#))))}

     dtype-proto/PToIterable
     {:convertible-to-iterable? (constantly true)
      :->iterable (fn [item# options#] (dtype-proto/->reader item# options#))}

     dtype-proto/PToMutable
     {:convertible-to-mutable? (constantly true)
      :->mutable
      (fn [list-item# options#]
        (-> (mutable/make-list-mutable list-item#
                                       (casting/safe-flatten ~datatype)
                                       ~datatype
                                       true)
            (dtype-proto/->mutable options#)))}

     dtype-proto/PRemoveRange
     {:remove-range!
      (fn [list-item# idx# count#]
        (let [list-item# (datatype->list-cast-fn ~datatype list-item#)]
          (.removeElements list-item# idx# (+ (int idx#) (int count#)))))}

     dtype-proto/PInsertBlock
     {:insert-block!
      (fn [list-item# idx# values# options#]
        (let [list-item# (datatype->list-cast-fn ~datatype list-item#)
              ary-data# (dtype-proto/->sub-array values#)
              idx# (int idx#)]
          ;;first, try array as they are most general
          (if (and ary-data#
                   (= ~datatype (casting/flatten-datatype
                                 (dtype-proto/get-datatype (:java-array ary-data#)))))
            (.addElements list-item# idx#
                          (typecast/datatype->array-cast-fn
                           ~datatype (:java-array ary-data#))
                          (int (:offset ary-data#))
                          (int (:length ary-data#)))
            ;;next, try list.
            (let [list-values# (when (dtype-proto/list-convertible? values#)
                                 (dtype-proto/->list-backing-store values#))]
              (if (and list-values#
                       (= ~datatype (casting/flatten-datatype
                                     (dtype-proto/get-datatype list-values#))))
                (.addAll list-item# idx# (datatype->list-cast-fn ~datatype list-values#))
                ;;fallback to element by element
                (let [item-reader# (typecast/datatype->reader
                                    ~datatype values# (:unchecked? options#))
                      n-values# (.lsize item-reader#)]
                  (parallel-for/serial-for
                   iter-idx# n-values#
                   (.add list-item# (+ idx# iter-idx#)
                         (casting/datatype->cast-fn
                          ~(casting/safe-flatten datatype)
                          ~datatype
                          (.read item-reader# iter-idx#))))))))))}))


(extend-list ByteList :int8)
(extend-list ShortList :int16)
(extend-list IntList :int32)
(extend-list LongList :int64)
(extend-list FloatList :float32)
(extend-list DoubleList :float64)
(extend-list BooleanList :boolean)
(extend-list ObjectList :object)


(defn make-list
  ([datatype elem-count-or-seq options]
   (-> (dtype-array/make-array-of-type datatype elem-count-or-seq options)
       dtype-proto/->list-backing-store))
  ([datatype elem-count-or-seq]
   (make-list datatype elem-count-or-seq {})))


(defmethod dtype-proto/make-container :list
  [container-type datatype elem-count-or-seq options]
  (let [host-datatype? (= datatype (casting/host-flatten datatype))
        typed-buf (if (or (number? elem-count-or-seq)
                          (instance? RandomAccess elem-count-or-seq)
                          (and (not (instance? Iterable elem-count-or-seq))
                               (satisfies? dtype-proto/PToReader elem-count-or-seq)))
                    (-> (typed-buffer/make-typed-buffer
                         datatype
                         elem-count-or-seq options)
                        (#(.backing-store ^TypedBuffer %))
                        dtype-proto/->list-backing-store)
                    (let [list-data (make-list (casting/host-flatten datatype) 0)]
                      (iterable-to-list/iterable->list elem-count-or-seq
                                                       list-data
                                                       {:unchecked? (:unchecked? options)
                                                        :datatype datatype})))]
    (if host-datatype?
      typed-buf
      (typed-buffer/set-datatype typed-buf datatype))))
