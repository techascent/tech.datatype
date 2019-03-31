(ns tech.datatype.list
  (:require [tech.datatype.base :as base]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.array :as dtype-array]
            [tech.datatype.typed-buffer :as typed-buffer]
            [tech.datatype.reader :refer [make-buffer-reader] :as reader]
            [tech.datatype.writer :refer [make-buffer-writer] :as writer]
            [tech.datatype.casting :as casting]
            [tech.datatype.nio-access :refer [buf-put buf-get
                                              datatype->pos-fn
                                              datatype->read-fn
                                              datatype->write-fn
                                              datatype->list-read-fn]]
            [tech.datatype.typecast :as typecast]
            [tech.datatype.mutable :as mutable]
            [clojure.core.matrix.protocols :as mp]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.parallel :as parallel])
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


(defn wrap-array
  [src-data]
  (if (satisfies? dtype-proto/PDatatype src-data)
    (case (base/get-datatype src-data)
      :int8 (ByteArrayList/wrap ^bytes src-data)
      :int16 (ShortArrayList/wrap ^shorts src-data)
      :int32 (IntArrayList/wrap ^ints src-data)
      :int64 (LongArrayList/wrap ^longs src-data)
      :float32 (FloatArrayList/wrap ^floats src-data)
      :float64 (DoubleArrayList/wrap ^doubles src-data)
      :boolean (BooleanArrayList/wrap ^booleans src-data)
      (ObjectArrayList/wrap (as-object-array src-data)))
    (ObjectArrayList/wrap (as-object-array src-data))))


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


     mp/PElementCount
     {:element-count (fn [item#]
                       (-> (datatype->list-cast-fn ~datatype item#)
                           (.size)))}

     dtype-proto/PToList
     {:->list-backing-store (fn [item#] item#)}))


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
                         (dtype-proto/copy-raw->item! (dtype-proto/->buffer-backing-store raw-data#)
                                                      ary-target# target-offset# options#))}

     dtype-proto/PToNioBuffer
     {:->buffer-backing-store (fn [item#]
                                (let [item# (datatype->array-list-cast-fn ~datatype item#)]
                                  (datatype->buffer-creation-length ~datatype (.elements item#) (.size item#))))}

     dtype-proto/PBuffer
     {:sub-buffer (fn [buffer# offset# length#]
                    (dtype-proto/sub-buffer (dtype-proto/->buffer-backing-store buffer#)
                                            offset# length#))
      :alias? (fn [lhs-dev-buffer# rhs-dev-buffer#]
                (dtype-proto/alias? (dtype-proto/->buffer-backing-store lhs-dev-buffer#)
                                    (dtype-proto/->buffer-backing-store rhs-dev-buffer#)))
      :partially-alias? (fn [lhs-dev-buffer# rhs-dev-buffer#]
                          (dtype-proto/partially-alias? (dtype-proto/->buffer-backing-store lhs-dev-buffer#)
                                                        (dtype-proto/->buffer-backing-store rhs-dev-buffer#)))}

     dtype-proto/PToArray
     {:->sub-array (fn [item#]
                     (dtype-proto/->sub-array (dtype-proto/->buffer-backing-store item#)))
      :->array-copy (fn [item#]
                      (.toArray (datatype->array-list-cast-fn ~datatype item#)))}
     dtype-proto/PToWriter
     {:->writer-of-type (fn [item# datatype# unchecked?#]
                          (-> (dtype-proto/->buffer-backing-store item#)
                              (dtype-proto/->writer-of-type datatype# unchecked?#)))}

     dtype-proto/PToReader
     {:->reader-of-type (fn [item# datatype# unchecked?#]
                          (-> (dtype-proto/->buffer-backing-store item#)
                              (dtype-proto/->reader-of-type datatype# unchecked?#)))}))


(extend-numeric-list ByteArrayList :int8)
(extend-numeric-list ShortArrayList :int16)
(extend-numeric-list IntArrayList :int32)
(extend-numeric-list LongArrayList :int64)
(extend-numeric-list FloatArrayList :float32)
(extend-numeric-list DoubleArrayList :float64)


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
                      (.subList list-data# offset# (+ offset# length#))))
      :alias? (fn [lhs-buffer# rhs-buffer#]
                (identical? (dtype-proto/->list-backing-store lhs-buffer#)
                            (dtype-proto/->list-backing-store rhs-buffer#)))
      :partially-alias? (fn [lhs-buffer# rhs-buffer#]
                          (dtype-proto/alias? lhs-buffer# rhs-buffer#))}

     dtype-proto/PToArray
     {:->sub-array (fn [item#]
                     (when-let [ary-list# (datatype->as-array-list ~datatype item#)]
                       (let [dst-ary# (.elements ary-list#)]
                         {:array-data dst-ary#
                          :offset 0
                          :length (.size ary-list#)})))
      :->array-copy (fn [item#]
                      (.toArray (datatype->list-cast-fn ~datatype item#)))}
     dtype-proto/PToWriter
     {:->writer-of-type
      (fn [item# writer-datatype# unchecked?#]
        (if (= writer-datatype# ~datatype)
          (writer/make-list-writer item# unchecked?#)
          (-> (writer/make-list-writer item# true)
              (dtype-proto/->writer-of-type writer-datatype# unchecked?#))))}

     dtype-proto/PToReader
     {:->reader-of-type
      (fn [item# reader-datatype# unchecked?#]
        (cond-> (reader/make-list-reader item#)
          (not= reader-datatype# ~datatype)
          (dtype-proto/->reader-of-type reader-datatype# unchecked?#)))}

     dtype-proto/PToMutable
     {:->mutable-of-type
      (fn [list-item# mut-dtype# unchecked?#]
        (if-let [mutable-fn# (get mutable/list-mutable-table
                                  [~datatype (casting/flatten-datatype mut-dtype#)])]
          (mutable-fn# list-item# unchecked?#)
          (throw (ex-info (format "Failed to find mutable %s->%s"
                                  ~datatype mut-dtype#) {}))))}

     dtype-proto/PInsertBlock
     {:insert-block!
      (fn [list-item# idx# values# options#]
        (let [list-item# (datatype->list-cast-fn ~datatype list-item#)
              ary-data# (dtype-proto/->sub-array values#)
              idx# (int idx#)]
          ;;first, try array as they are most general
          (if (and ary-data#
                   (= ~datatype (casting/flatten-datatype
                                 (dtype-proto/get-datatype (:array-data ary-data#)))))
            (.addElements list-item# idx#
                          (typecast/datatype->array-cast-fn ~datatype (:array-data ary-data#))
                          (int (:offset ary-data#))
                          (int (:length ary-data#)))
            ;;next, try list.
            (let [list-values# (dtype-proto/->list-backing-store values#)]
              (if (and list-values#
                       (= ~datatype (casting/flatten-datatype
                                     (dtype-proto/get-datatype list-values#))))
                (.addAll list-item# idx# (datatype->list-cast-fn ~datatype list-values#))
                ;;fallback to element by element
                (let [item-reader# (typecast/datatype->reader ~datatype values# (:unchecked? options#))
                      n-values# (.size item-reader#)]
                  (c-for [iter-idx# (int 0) (< iter-idx# n-values#) (inc iter-idx#)]
                         (.add list-item# (+ idx# iter-idx#) (.read item-reader# iter-idx#)))))))))}))


(extend-list ByteList :int8)
(extend-list ShortList :int16)
(extend-list IntList :int32)
(extend-list LongList :int64)
(extend-list FloatList :float32)
(extend-list DoubleList :float64)
(extend-list BooleanList :boolean)
(extend-list ObjectList :object)


(defmacro extend-array-with-list
  [ary-type]
  `(clojure.core/extend
       ~ary-type
     dtype-proto/PToList
     {:->list-backing-store (fn [item#] (wrap-array item#))}))

(extend-array-with-list (Class/forName "[B"))
(extend-array-with-list (Class/forName "[S"))
(extend-array-with-list (Class/forName "[I"))
(extend-array-with-list (Class/forName "[J"))
(extend-array-with-list (Class/forName "[F"))
(extend-array-with-list (Class/forName "[D"))
(extend-array-with-list (Class/forName "[Z"))


(defn make-list
  ([datatype elem-count-or-seq options]
   (-> (dtype-array/make-array-of-type datatype elem-count-or-seq options)
       wrap-array))
  ([datatype elem-count-or-seq]
   (make-list datatype elem-count-or-seq {})))


(defmethod dtype-proto/make-container :list
  [container-type datatype elem-count-or-seq options]
  (let [typed-buf (if (or (instance? RandomAccess elem-count-or-seq)
                          (satisfies? dtype-proto/PToReader elem-count-or-seq)
                          (number? elem-count-or-seq)
                          (not (instance? Iterable elem-count-or-seq)))
                    (typed-buffer/make-typed-buffer
                     datatype
                     elem-count-or-seq options)
                    (let [list-data (make-list (casting/host-flatten datatype) 0)]
                      (mutable/iterable->list elem-count-or-seq
                                              list-data
                                              {:unchecked? (:unchecked? options)
                                               :datatype datatype})))]
    (-> (dtype-proto/->list-backing-store typed-buf)
        (typed-buffer/set-datatype datatype))))
