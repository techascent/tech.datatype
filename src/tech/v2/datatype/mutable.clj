(ns tech.v2.datatype.mutable
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
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
            [tech.v2.datatype.typecast :as typecast]
            [clojure.core.matrix.protocols :as mp]
            [tech.v2.datatype.iterator :as dtype-iter])
  (:import [tech.v2.datatype ObjectMutable ByteMutable
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
  [mutable-cls buffer-cls buffer mutable-dtype
   intermediate-dtype buffer-dtype unchecked?]
  `(if ~unchecked?
     (reify ~mutable-cls
       (getDatatype [mut-item#] ~intermediate-dtype)
       (lsize [mut-item#] (.size ~buffer))
       (insert [mut-item# idx# value#]
         (.add ~buffer idx# (unchecked-full-cast
                             value# ~mutable-dtype ~intermediate-dtype ~buffer-dtype)))
       (append [mut-item# ~'value]
         (.add ~buffer ~(if (= buffer-dtype :object)
                          `(identity ~'value)
                          `(unchecked-full-cast
                            ~'value ~mutable-dtype ~intermediate-dtype ~buffer-dtype))))
       (remove [mut-item# idx#]
         (datatype->single-remove-fn ~buffer-dtype ~buffer idx#)))
     (reify ~mutable-cls
       (getDatatype [mut-item#] ~intermediate-dtype)
       (lsize [mut-item#] (.size ~buffer))
       (insert [mut-item# idx# value#]
         (.add ~buffer idx# (checked-full-write-cast
                             value# ~mutable-dtype ~intermediate-dtype ~buffer-dtype)))
       (append [mut-item# ~'value]
         (.add ~buffer ~(if (= buffer-dtype :object)
                          `(identity ~'value)
                          `(checked-full-write-cast
                            ~'value ~mutable-dtype ~intermediate-dtype ~buffer-dtype))))
       (remove [mut-item# idx#]
         (datatype->single-remove-fn ~buffer-dtype ~buffer idx#)))))


(defmacro make-list-mutable-table
  []
  `(->> [~@(for [dtype casting/base-datatypes
                 buffer-datatype casting/all-host-datatypes]
             [[buffer-datatype dtype]
              `(fn [buffer# unchecked?#]
                 (let [buffer# (typecast/datatype->list-cast-fn
                                ~buffer-datatype buffer#)]
                  (make-mutable
                   ~(typecast/datatype->mutable-type dtype)
                   ~(typecast/datatype->list-type buffer-datatype)
                   buffer#
                   ~(casting/datatype->safe-host-type dtype) ~dtype
                   ~buffer-datatype
                   unchecked?#)))])]
        (into {})))


(def list-mutable-table (make-list-mutable-table))


(defmacro reify-marshalling-mutable
  [outer-mutable-cls outer-dtype intermediate-dtype
   inner-dtype inner-mutable unchecked?]
  `(if ~unchecked?
     (reify ~outer-mutable-cls
       (getDatatype [item#] ~intermediate-dtype)
       (lsize [mut-item#] (.lsize ~inner-mutable))
       (insert [item# idx# value#]
         (.insert ~inner-mutable idx#
                  (unchecked-full-cast value#
                                       ~outer-dtype
                                       ~intermediate-dtype
                                       ~inner-dtype)))
       (append [mut-item# value#]
         (.append ~inner-mutable
                  (unchecked-full-cast value#
                                       ~outer-dtype
                                       ~intermediate-dtype
                                       ~inner-dtype)))
       (remove [item# idx#]
         (.remove ~inner-mutable idx#)))
     (reify ~outer-mutable-cls
       (getDatatype [item#] ~intermediate-dtype)
       (lsize [mut-item#] (.lsize ~inner-mutable))
       (insert [item# idx# value#]
         (.insert ~inner-mutable idx#
                  (checked-full-write-cast value# ~outer-dtype
                                           ~outer-dtype ~inner-dtype)))
       (append [mut-item# value#]
         (.append ~inner-mutable
                  (checked-full-write-cast value#
                                           ~outer-dtype
                                           ~intermediate-dtype
                                           ~inner-dtype)))
       (remove [item# idx#]
         (.remove ~inner-mutable idx#)))))


(defmacro make-marshal-mutable-table
  []
  `(->> [~@(for [dtype casting/base-datatypes
                 buffer-datatype casting/all-host-datatypes]
             [[buffer-datatype dtype]
              `(fn [buffer# unchecked?#]
                 (let [buffer# (typecast/datatype->mutable ~buffer-datatype buffer#
                                                           unchecked?#)]
                   (reify-marshalling-mutable
                    ~(typecast/datatype->mutable-type dtype)
                    ~(casting/datatype->safe-host-type dtype)
                     ~dtype
                     ~buffer-datatype
                     buffer#
                    unchecked?#)))])]
        (into {})))


(def marshalling-mutable-table (make-marshal-mutable-table))


(defmacro extend-mutable
  [datatype]
  `(clojure.core/extend
       ~(typecast/datatype->mutable-type datatype)
     dtype-proto/PToMutable
     {:->mutable-of-type
      (fn [item# mut-dtype# unchecked?#]
        (if (= mut-dtype# (dtype-proto/get-datatype item#))
          item#
          (if-let [mutable-fn# (get marshalling-mutable-table
                                    [~datatype (casting/flatten-datatype mut-dtype#)])]
            (mutable-fn# item# unchecked?#)
            (throw (ex-info (format "Failed to find marshalling mutable: %s %s"
                                    ~datatype mut-dtype#)
                            {:src-datatype ~datatype
                             :dst-datatype mut-dtype#})))))}))

(extend-mutable :int8)
(extend-mutable :int16)
(extend-mutable :int32)
(extend-mutable :int64)
(extend-mutable :float32)
(extend-mutable :float64)
(extend-mutable :boolean)
(extend-mutable :object)



(defmacro make-iter->list-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             [dtype `(fn [iter# output# unchecked?#]
                       (let [iter# (typecast/datatype->iter ~dtype iter# unchecked?#)
                             output# (or output#
                                         (dtype-proto/make-container
                                          :list ~dtype 0))
                             mutable# (typecast/datatype->mutable ~dtype output#)]
                         (while (.hasNext iter#)
                           (.append mutable# (typecast/datatype->iter-next-fn
                                              ~dtype iter#)))
                         output#))])]
        (into {})))


(def iter->list-table (make-iter->list-table))


(defn iterable->list
  [src-iterable dst-list {:keys [datatype unchecked?]}]
  (let [datatype (or datatype (dtype-proto/get-datatype src-iterable))
        dst-list (or dst-list (dtype-proto/make-container :list datatype 0 {}))
        iter-fn (get iter->list-table (casting/flatten-datatype datatype))]
    (iter-fn src-iterable dst-list unchecked?)))
