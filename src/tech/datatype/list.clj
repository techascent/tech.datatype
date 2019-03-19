(ns tech.datatype.list
  (:require [tech.datatype.base :as base]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype :as dtype]
            [tech.datatype.array :as dtype-array]
            [tech.datatype.reader :as reader]
            [tech.datatype.writer :as writer]
            [tech.datatype.casting :as casting]
            [tech.datatype.nio-access :refer [buf-put buf-get
                                              datatype->pos-fn
                                              datatype->read-fn
                                              datatype->write-fn
                                              datatype->list-read-fn]]
            [clojure.core.matrix.protocols :as mp]
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
           [java.util List ArrayList Arrays]
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
    (case (dtype/get-datatype src-data)
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
     {:->array (fn [item#]
                 (dtype-proto/->array (dtype-proto/->buffer-backing-store item#)))
      :->sub-array (fn [item#]
                     (dtype-proto/->sub-array (dtype-proto/->buffer-backing-store item#)))
      :->array-copy (fn [item#]
                      (.toArray (datatype->array-list-cast-fn ~datatype item#)))}
     dtype-proto/PToWriter
     {:->object-writer (fn [item#] (writer/->marshalling-writer item# :object true))

      :->writer-of-type (fn [item# datatype# unchecked?#]
                          (-> (dtype-proto/->buffer-backing-store item#)
                              (dtype-proto/->writer-of-type datatype# unchecked?#)))}

     dtype-proto/PToReader
     {:->object-reader (fn [item#] (reader/->marshalling-reader item# :object true))

      :->reader-of-type (fn [item# datatype# unchecked?#]
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


(defmacro make-list-writer
  [writer-type buffer
   datatype]
  `(reify ~writer-type
     (write [writer# idx# value#]
       (.set ~buffer idx# value#))
     (writeConstant [writer# offset# value# count#]
       (parallel/parallel-for
        idx# count#
        (.set ~buffer idx# value#)))
     (writeBlock [writer# offset# values#]
       (let [values-pos# (datatype->pos-fn ~datatype values#)
             count# (base/ecount values#)]
         (parallel/parallel-for
          idx# count#
          (.set ~buffer (+ idx# offset#)
                (datatype->read-fn ~datatype values# idx# values-pos#)))))
     (writeIndexes [writer# indexes# values#]
       (let [n-elems# (base/ecount indexes#)
             idx-pos# (.position indexes#)
             val-pos# (datatype->pos-fn ~datatype values#)]
         (parallel/parallel-for
          idx#
          n-elems#
          (.set ~buffer (buf-get indexes# idx# idx-pos#)
                (datatype->read-fn ~datatype values# idx# val-pos#)))))))


(defmacro make-list-reader
  [reader-type buffer datatype]
  `(reify ~reader-type
     (read [reader# idx#]
       (datatype->list-read-fn ~datatype ~buffer idx#))
     (readBlock [reader# offset# dest#]
       (let [dest-pos# (datatype->pos-fn ~datatype dest#)
             count# (base/ecount dest#)]
         (parallel/parallel-for
          idx# count#
          (datatype->write-fn ~datatype dest# idx# dest-pos#
                              (datatype->list-read-fn ~datatype ~buffer
                                                      (+ idx# offset#))))))

     (readIndexes [reader# indexes# dest#]
       (let [idx-pos# (.position indexes#)
             dest-pos# (datatype->pos-fn ~datatype dest#)
             count# (base/ecount dest#)]
         (parallel/parallel-for
          idx# count#
          (datatype->write-fn ~datatype dest# idx# dest-pos#
                              (datatype->list-read-fn ~datatype ~buffer
                                                      (buf-get indexes# idx# idx-pos#))))))))



(defmacro extend-list
  [typename datatype]
  `(clojure.core/extend
       ~typename
     dtype-proto/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (base/raw-dtype-copy! raw-data# ary-target# target-offset# options#))}

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
                          false)}

     dtype-proto/PToArray
     {:->array (fn [item#]
                 (when-let [ary-list# (datatype->as-array-list ~datatype item#)]
                   (let [dst-ary# (.elements ary-list#)]
                     (when (= (alength dst-ary#) (.size ary-list#))
                       dst-ary#))))
      :->sub-array (fn [item#]
                     (when-let [ary-list# (datatype->as-array-list ~datatype item#)]
                       (let [dst-ary# (.elements ary-list#)]
                         {:array-data dst-ary#
                          :offset 0
                          :length (.size ary-list#)})))
      :->array-copy (fn [item#]
                      (.toArray (datatype->list-cast-fn ~datatype item#)))}
     dtype-proto/PToWriter
     {:->object-writer (fn [item#] (writer/->marshalling-writer item# :object true))

      :->writer-of-type (fn [item# datatype# unchecked?#]
                          (let [~'item (datatype->list-cast-fn ~datatype item#)
                                src-writer#
                                ~(case datatype
                                   :int8 `(make-list-writer ByteWriter ~'item :int8)
                                   :int16 `(make-list-writer ShortWriter ~'item :int16)
                                   :int32 `(make-list-writer IntWriter ~'item :int32)
                                   :int64 `(make-list-writer LongWriter ~'item :int64)
                                   :float32 `(make-list-writer FloatWriter ~'item :float32)
                                   :float64 `(make-list-writer DoubleWriter ~'item :float64)
                                   :boolean `(make-list-writer BooleanWriter ~'item :boolean)
                                   :object `(make-list-writer ObjectWriter ~'item :object)
                                   )]
                            (writer/->marshalling-writer src-writer# datatype# unchecked?#)))}

     dtype-proto/PToReader
     {:->object-reader (fn [item#] (reader/->marshalling-reader item# :object true))

      :->reader-of-type (fn [item# datatype# unchecked?#]
                          (let [~'item (datatype->list-cast-fn ~datatype item#)
                                src-reader#
                                ~(case datatype
                                   :int8 `(make-list-reader ByteReader ~'item :int8)
                                   :int16 `(make-list-reader ShortReader ~'item :int16)
                                   :int32 `(make-list-reader IntReader ~'item :int32)
                                   :int64 `(make-list-reader LongReader ~'item :int64)
                                   :float32 `(make-list-reader FloatReader ~'item :float32)
                                   :float64 `(make-list-reader DoubleReader ~'item :float64)
                                   :boolean `(make-list-reader BooleanReader ~'item :boolean)
                                   :object `(make-list-reader ObjectReader ~'item :object)
                                   )]
                            (reader/->marshalling-reader src-reader# datatype# unchecked?#)))}))

(extend-list ByteList :int8)
(extend-list ShortList :int16)
(extend-list IntList :int32)
(extend-list LongList :int64)
(extend-list FloatList :float32)
(extend-list DoubleList :float64)
(extend-list BooleanList :boolean)
(extend-list ObjectList :object)


(extend-type (Class/forName "[Z")
  dtype-proto/PToList
  (->list-backing-store [item]
    (wrap-array item)))


(defn make-list
  ([datatype elem-count-or-seq options]
   (-> (dtype-array/make-array-of-type datatype elem-count-or-seq options)
       wrap-array))
  ([datatype elem-count-or-seq]
   (make-list datatype elem-count-or-seq {})))


(defmethod dtype-proto/make-container :fastutil-list
  [container-type datatype elem-count-or-seq options]
  (make-list datatype elem-count-or-seq options))


;; (defrecord TypedList [base-data datatype]
;;   dtype-proto/PDatatype
;;   (get-datatype [_] datatype)

;;   dtype-proto/PCopyRawData
;;   (copy-raw->item! [raw-data ary-target target-offset options]
;;     (base/copy-raw->item! (unsigned/->typed-buffer raw-data) ary-target target-offset options))

;;   dtype-proto/PPrototype
;;   (from-prototype [item datatype shape]
;;     (->TypedList (base/from-prototype base-data datatype shape) datatype))

;;   dtype-proto/PClone
;;   (clone [item datatype]
;;     (let [retval (base/from-prototype item datatype (dtype/shape item))]
;;       (dtype/copy! item retval)))

;;   dtype-proto/PToBuffer
;;   (->buffer-backing-store [item] (dtype-proto/->buffer-backing-store base-data))

;;   dtype-proto/POffsetable
;;   (offset-item [item offset]
;;     (dtype-proto/offset-item (unsigned/->typed-buffer item) offset))

;;   dtype-proto/PToArray
;;   (->array [item]
;;     (dtype-proto/->array (unsigned/->typed-buffer item)))
;;   (->array-copy [item]
;;     (dtype-proto/->array-copy (unsigned/->typed-buffer item)))

;;   mp/PElementCount
;;   (element-count [_] (mp/element-count base-data))

;;   dtype-proto/PBuffer
;;   (sub-buffer [buffer offset length]
;;     (dtype-proto/sub-buffer (unsigned/->typed-buffer buffer) offset length))
;;   (alias? [lhs-buffer rhs-buffer]
;;     (dtype-proto/alias? (unsigned/->typed-buffer lhs-buffer) rhs-buffer))
;;   (partially-alias? [lhs-buffer rhs-buffer]
;;     (dtype-proto/partially-alias? (unsigned/->typed-buffer lhs-buffer) rhs-buffer))

;;   PListConvertible
;;   (->list-list-backing-store [item] (->list-list-backing-store base-data))


;;   PDataMutate
;;   (insert! [item idx value]
;;     (insert! base-data idx (unsigned/jvm-cast value datatype)))
;;   (append! [item value]
;;     (append! base-data (unsigned/jvm-cast value datatype)))
;;   (insert-constant! [item idx value n-elems]
;;     (insert-constant! base-data idx (unsigned/jvm-cast value datatype) n-elems))
;;   (insert-elems! [item idx elem-buf]
;;     (insert-elems! base-data idx elem-buf))
;;   (append-elems! [item elem-buf]
;;     (append-elems! base-data elem-buf))
;;   (remove-range! [item start-idx n-elems]
;;     (remove-range! base-data start-idx n-elems)))


;; (defn make-typed-list
;;   "Make a 'list' datatype that supports unsigned types"
;;   ([datatype elem-count-or-seq options]
;;    ;;do conversions, load data
;;    (-> (unsigned/make-typed-buffer datatype elem-count-or-seq options)
;;        (dtype-proto/->buffer-backing-store)
;;        (dtype-proto/->array)
;;        (wrap-array)
;;        (->TypedList datatype)))
;;   ([datatype elem-count-or-seq]
;;    (make-typed-list datatype elem-count-or-seq {})))
