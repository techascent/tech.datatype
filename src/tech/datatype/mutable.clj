(ns tech.datatype.mutable
  (:require [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.list :as dtype-list]
            [tech.datatype.nio-access
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
            [tech.datatype.typecast :as typecast]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.datatype.io :as dtype-io]
            [clojure.core.matrix.protocols :as mp])
  (:import [tech.datatype ObjectMutable ByteMutable
            ShortMutable IntMutable LongMutable
            FloatMutable DoubleMutable BooleanMutable]
           [it.unimi.dsi.fastutil.bytes ByteList ByteArrayList]
           [it.unimi.dsi.fastutil.shorts ShortList ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntList IntArrayList]
           [it.unimi.dsi.fastutil.longs LongList LongArrayList]
           [it.unimi.dsi.fastutil.floats FloatList FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleList DoubleArrayList]
           [it.unimi.dsi.fastutil.booleans BooleanList BooleanArrayList]
           [it.unimi.dsi.fastutil.objects ObjectList ObjectArrayList]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro datatype->single-remove-fn
  [datatype item idx]
  (case datatype
    :int8 `(.removeByte ~item ~idx)
    :int16 `(.removeShort ~item ~idx)
    :int32 `(.removeInt ~item ~idx)
    :int64 `(.removeLong ~item ~idx)
    :float32 `(.removeFloat ~item ~idx)
    :float64 `(.removeDouble ~item ~idx)
    :boolean `(.removeBoolean ~item ~idx)
    :object `(.remove ~item ~idx)))



(defmacro make-mutable
  [mutable-cls buffer-cls buffer mutable-dtype intermediate-dtype buffer-dtype unchecked?]
  `(if ~unchecked?
     (reify ~mutable-cls
       (insert [mut-item# idx# value#]
         (.add ~buffer idx# (unchecked-full-cast
                             value# ~mutable-dtype ~intermediate-dtype ~buffer-dtype)))
       (insertConstant [mut-item# offset# value# count#]
         (let [value# (unchecked-full-cast value# ~mutable-dtype ~intermediate-dtype ~buffer-dtype)]
           (c-for [idx# (int 0) (< idx# count#) (inc idx#)]
                  (.add ~buffer offset# value#))))
       (insertBlock [mut-item# ~'idx ~'values]
         (let [~'values (typecast/datatype->buffer-cast-fn
                         ~buffer-dtype
                         (dtype-io/make-transfer-buffer ~'values ~intermediate-dtype
                                                        ~buffer-dtype true ~unchecked?))]
           ~(cond
              (casting/numeric-type? buffer-dtype)
              `(let [ary-data# (dtype-proto/->sub-array ~'values)]
                 (.addElements ~buffer ~'idx
                               (typecast/datatype->array-cast-fn
                                ~buffer-dtype
                                (:array-data ary-data#))
                               (int (:offset ary-data#))
                               (int (:length ary-data#))))
              :else
              `(.addAll ~buffer ~'idx ~'values))))
       (remove [mut-item# idx#]
         (datatype->single-remove-fn ~buffer-dtype ~buffer idx#))
       (removeRange [mut-item# idx# count#]
         (.removeElements ~buffer idx# (+ idx# count#))))
     (reify ~mutable-cls
       (insert [mut-item# idx# value#]
         (.add ~buffer idx# (checked-full-write-cast
                             value# ~mutable-dtype ~intermediate-dtype ~buffer-dtype)))
       (insertConstant [mut-item# offset# value# count#]
         (let [value# (checked-full-write-cast value# ~mutable-dtype ~intermediate-dtype ~buffer-dtype)]
           (c-for [idx# (int 0) (< idx# count#) (inc idx#)]
                  (.add ~buffer offset# value#))))
       (insertBlock [mut-item# ~'idx ~'values]
         (let [~'values (typecast/datatype->buffer-cast-fn
                         ~buffer-dtype
                         (dtype-io/make-transfer-buffer ~'values ~intermediate-dtype
                                                        ~buffer-dtype true ~unchecked?))]
           ~(cond
              (casting/numeric-type? buffer-dtype)
              `(let [ary-data# (dtype-proto/->sub-array ~'values)]
                 (.addElements ~buffer ~'idx
                               (typecast/datatype->array-cast-fn ~buffer-dtype
                                                                 (:array-data ary-data#))
                               (int (:offset ary-data#))
                               (int (:length ary-data#))))
              :else
              `(.addAll ~buffer ~'idx ~'values))))
       (remove [mut-item# idx#]
         (datatype->single-remove-fn ~buffer-dtype ~buffer idx#))
       (removeRange [mut-item# idx# count#]
         (.removeElements ~buffer idx# (+ idx# count#))))))


(defmacro reify-marshalling-mutable
  [outer-mutable-cls outer-datatype intermediate-datatype
   inner-datatype inner-mutable unchecked?]
  `(if ~unchecked?
     (reify ~outer-mutable-cls
       (insert [item# idx# value#]
         (.insert ~inner-mutable idx#
                  (unchecked-full-cast value#
                                       ~outer-datatype
                                       ~intermediate-datatype
                                       ~inner-datatype)))
       (insertConstant [item# idx# value# count#]
         (.insertConstant ~inner-mutable idx#
                          (unchecked-full-cast value#
                                               ~outer-datatype
                                               ~intermediate-datatype
                                               ~inner-datatype)
                          count#))
       (insertBlock [item# idx# values#]
         (.insertBlock ~inner-mutable idx#
                       (typecast/datatype->buffer-cast-fn
                        ~inner-datatype
                        (dtype-io/make-transfer-buffer ~'values ~intermediate-datatype
                                                       ~inner-datatype true ~unchecked?))
                       (dtype-io/dense-copy!
                        (typcast/make-interface-buffer-type
                         ~inner-datatype (mp/element-count values#))
                        values#
                        ~unchecked?)))
       (remove [item# idx#]
         (.remove ~inner-mutable idx#))
       (removeRange [idx# count#]
         (.removeRange ~inner-mutable odx# count#)))
     (reify ~outer-mutable-cls
       (insert [item# idx# value#]
         (.insert ~inner-mutable idx#
                  (checked-full-write-cast value# ~outer-datatype
                                           ~outer-datatype ~inner-datatype)))
       (insertConstant [item# idx# value# count#]
         (.insertConstant ~inner-mutable idx#
                          (checked-full-write-cast value# ~outer-datatype
                                                   ~outer-datatype ~inner-datatype)
                          count#))
       (insertBlock [item# idx# values#]
         (.insertBlock ~inner-mutable idx#
                       (dtype-io/dense-copy!
                        (typcast/make-interface-buffer-type
                         ~inner-datatype (mp/element-count values#))
                        values#
                        ~unchecked?)))
       (remove [item# idx#]
         (.remove ~inner-mutable idx#))
       (removeRange [idx# count#]
         (.removeRange ~inner-mutable odx# count#)))))


(defmacro extend-mutable
  [typename datatype]
  `(clojure.core/extend
       ~typename
     dtype-proto/PDatatype
     {:get-datatype (fn [item#] ~datatype)}
     dtype-proto/PToMutable
     {:->mutable-of-type
      (fn [item# mut-dtype# unchecked?#]
        (if (= mut-dtype# ~datatype)
          item#
          (let [item# (casting/datatype->mutable ~datatype item#)]
            (case mut-dtype#
              :int8 (reify-marshalling-mutable ByteMutable :int8 :int8 ~datatype item# unchecked?#)
              :uint8 (reify-marshalling-mutable ShortMutable :int16 :uint8 ~datatype item# unchecked?#)
              :int16 (reify-marshalling-mutable ShortMutable :int16 :int16 ~datatype item# unchecked?#)
              :uint16 (reify-marshalling-mutable IntMutable :int32 :uint16 ~datatype item# unchecked?#)
              :int32 (reify-marshalling-mutable IntMutable :int32 :int32 ~datatype item# unchecked?#)
              :uint32 (reify-marshalling-mutable LongMutable :int64 :uint32 ~datatype item# unchecked?#)
              :int64 (reify-marshalling-mutable LongMutable :int64 :int64 ~datatype item# unchecked?#)
              :uint64 (reify-marshalling-mutable LongMutable :int64 :uint64 ~datatype item# unchecked?#)
              :float32 (reify-marshalling-mutable FloatMutable :float32 :float32 ~datatype item# unchecked?#)
              :float64 (reify-marshalling-mutable DoubleMutable :float64 :float64 ~datatype item# unchecked?#)
              :boolean (reify-marshalling-mutable BooleanMutable :boolean :boolean ~datatype item# unchecked?#)
              :object (reify-marshalling-mutable BooleanMutable :object :object ~datatype item# unchecked?#)))))}))



(defmacro extend-list-mutable
  [typename base-mutable-class datatype]
  `(clojure.core/extend
       ~typename
     dtype-proto/PToMutable
     {:->mutable-of-type
      (fn [list-item# mut-dtype# unchecked?#]
        (let [list-item# (dtype-list/datatype->list-cast-fn ~datatype list-item#)]
          (-> (make-mutable ~base-mutable-class ~typename list-item# ~datatype ~datatype ~datatype true)
              (dtype-proto/->mutable-of-type mut-dtype# unchecked?#))))}))


(extend-list-mutable ByteList ByteMutable :int8)
(extend-list-mutable ShortList ShortMutable :int16)
(extend-list-mutable IntList IntMutable :int32)
(extend-list-mutable LongList LongMutable :int64)
(extend-list-mutable FloatList FloatMutable :float32)
(extend-list-mutable DoubleList DoubleMutable :float64)
(extend-list-mutable BooleanList BooleanMutable :boolean)
(extend-list-mutable ObjectList ObjectMutable :object)
