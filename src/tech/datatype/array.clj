(ns tech.datatype.array
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.base :as base]
            [tech.datatype.reader :as reader]
            [clojure.core.matrix.protocols :as mp]
            [tech.datatype.casting :as casting]
            [tech.jna :as jna])
  (:import [java.nio Buffer ByteBuffer ShortBuffer
            IntBuffer LongBuffer FloatBuffer DoubleBuffer]
           [java.lang.reflect Constructor]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn as-byte-array
  ^bytes [obj] obj)

(defn as-short-array
  ^shorts [obj] obj)

(defn as-int-array
  ^ints [obj] obj)

(defn as-long-array
  ^longs [obj] obj)

(defn as-float-array
  ^floats [obj] obj)

(defn as-double-array
  ^doubles [obj] obj)


(defmacro datatype->array-cast-fn
  [dtype buf]
  (condp = dtype
    :int8 `(as-byte-array ~buf)
    :int16 `(as-short-array ~buf)
    :int32 `(as-int-array ~buf)
    :int64 `(as-long-array ~buf)
    :float32 `(as-float-array ~buf)
    :float64 `(as-double-array ~buf)))


(declare make-array-of-type)


(defmacro datatype->buffer-creation
  [datatype src-ary]
  (case datatype
    :int8 `(ByteBuffer/wrap ^bytes ~src-ary)
    :int16 `(ShortBuffer/wrap ^shorts ~src-ary)
    :int32 `(IntBuffer/wrap ^ints ~src-ary)
    :int64 `(LongBuffer/wrap ^longs ~src-ary)
    :float32 `(FloatBuffer/wrap ^floats ~src-ary)
    :float64 `(DoubleBuffer/wrap ^doubles ~src-ary)))


(defmacro implement-numeric-array-type
  [ary-cls datatype]
  `(clojure.core/extend
       ~ary-cls
     dtype-proto/PDatatype
     {:get-datatype (fn [arg#] ~datatype)}
     dtype-proto/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (base/raw-dtype-copy! raw-data# ary-target# target-offset# options#))}
     dtype-proto/PPrototype
     {:from-prototype (fn [src-ary# datatype# shape#]
                        (when-not (= 1 (count shape#))
                          (throw (ex-info "arrays are 1 dimensional" {})))
                        (make-array-of-type datatype# (base/shape->ecount shape#)))}

     dtype-proto/PToNioBuffer
     {:->buffer-backing-store (fn [item#] (datatype->buffer-creation ~datatype item#))}

     dtype-proto/PToArray
     {:->array (fn [item#] item#)
      :->sub-array (fn [item#]
                     {:array-data item#
                      :offset 0
                      :length (base/ecount item#)})
      :->array-copy (fn [item#]
                      (base/copy! item# (make-array-of-type ~datatype (base/ecount item#))))}
     dtype-proto/PBuffer
     {:sub-buffer (fn [buffer# offset# length#]
                    (dtype-proto/sub-buffer (dtype-proto/->buffer-backing-store buffer#)
                                            offset# length#))

      :alias? (fn [lhs-buffer# rhs-buffer#]
                (dtype-proto/alias? (dtype-proto/->buffer-backing-store lhs-buffer#)
                                    rhs-buffer#))

      :partially-alias? (fn [lhs-buffer# rhs-buffer#]
                          (dtype-proto/partially-alias? (dtype-proto/->buffer-backing-store lhs-buffer#)
                                                        rhs-buffer#))}
     dtype-proto/PToReader
     {:->object-reader (fn [item#]
                         (dtype-proto/->object-reader (dtype-proto/->buffer-backing-store item#)))
      :->reader-of-type (fn [item# datatype# unchecked?#]
                          (dtype-proto/->reader-of-type (dtype-proto/->buffer-backing-store item#)
                                                        datatype# unchecked?#))}

     dtype-proto/PToWriter
     {:->object-writer (fn [item#]
                         (dtype-proto/->object-writer (dtype-proto/->buffer-backing-store item#)))
      :->writer-of-type (fn [item# datatype#]
                          (dtype-proto/->writer-of-type (dtype-proto/->buffer-backing-store item#)
                                                        datatype#))}))


(implement-numeric-array-type (Class/forName "[B") :int8)
(implement-numeric-array-type (Class/forName "[S") :int16)
(implement-numeric-array-type (Class/forName "[I") :int32)
(implement-numeric-array-type (Class/forName "[J") :int64)
(implement-numeric-array-type (Class/forName "[F") :float32)
(implement-numeric-array-type (Class/forName "[D") :float64)


(defonce ^:dynamic *array-constructors* (atom {}))


(defn add-array-constructor!
  [item-dtype cons-fn]
  (swap! *array-constructors* assoc item-dtype cons-fn)
  (keys @*array-constructors*))


(defn add-numeric-array-constructor
  [item-dtype ary-cons-fn]
  (add-array-constructor!
   item-dtype
   (fn [elem-count-or-seq options]
     (cond
       (number? elem-count-or-seq)
       (ary-cons-fn elem-count-or-seq)
       (satisfies? dtype-proto/PDatatype elem-count-or-seq)
       (if (and (satisfies? dtype-proto/PToArray elem-count-or-seq)
                (= item-dtype (base/get-datatype elem-count-or-seq)))
         (dtype-proto/->array-copy elem-count-or-seq)
         (let [n-elems (base/ecount elem-count-or-seq)]
           (base/copy! elem-count-or-seq 0
                       (ary-cons-fn n-elems) 0
                       n-elems options)))
       :else
       (let [elem-count-or-seq (if (or (number? elem-count-or-seq)
                                       (:unchecked? options))
                                 elem-count-or-seq
                                 (map #(casting/cast % item-dtype) elem-count-or-seq))]
         (ary-cons-fn elem-count-or-seq))))))


(add-numeric-array-constructor :int8 byte-array)
(add-numeric-array-constructor :int16 short-array)
(add-numeric-array-constructor :int32 int-array)
(add-numeric-array-constructor :int64 long-array)
(add-numeric-array-constructor :float32 float-array)
(add-numeric-array-constructor :float64 double-array)
(add-numeric-array-constructor :boolean boolean-array)


(defn make-object-array-of-type
  [obj-type elem-count-or-seq options]
  (let [elem-count-or-seq (if (or (number? elem-count-or-seq)
                                     (:unchecked? options))
                               elem-count-or-seq
                               (map (partial jna/ensure-type obj-type)
                                    elem-count-or-seq))]
    (if (number? elem-count-or-seq)
      (let [constructor (if (:construct? options)
                          (.getConstructor ^Class obj-type (make-array Class 0))
                          nil)]
        (if constructor
          (into-array obj-type (repeatedly (long elem-count-or-seq)
                                           #(.newInstance
                                             ^Constructor constructor
                                             (make-array Object 0))))
          (make-array obj-type (long elem-count-or-seq))))
      (into-array obj-type elem-count-or-seq))))


(defn make-array-of-type
  ([datatype elem-count-or-seq options]
   (if (instance? Class datatype)
     (make-object-array-of-type datatype elem-count-or-seq options)
     (if-let [cons-fn (get @*array-constructors* datatype)]
       (cons-fn elem-count-or-seq options)
       (throw (ex-info (format "Failed to find constructor for datatype %s" datatype)
                       {:datatype datatype})))))
  ([datatype elem-count-or-seq]
   (make-array-of-type datatype elem-count-or-seq {})))


(defmethod dtype-proto/make-container :java-array
  [container-type datatype elem-count-or-seq options]
  (make-array-of-type datatype elem-count-or-seq options))
