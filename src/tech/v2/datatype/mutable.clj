(ns tech.v2.datatype.mutable
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.nio-access
             :refer [unchecked-full-cast
                     checked-full-write-cast]]
            [tech.v2.datatype.typecast :as typecast])
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
  [mutable-cls _buffer-cls buffer mutable-dtype
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
       (mremove [mut-item# idx#]
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
       (mremove [mut-item# idx#]
         (datatype->single-remove-fn ~buffer-dtype ~buffer idx#)))))


(defmacro make-list-mutable-table
  []
  `(->> [~@(for [{:keys [intermediate-datatype
                         buffer-datatype
                         reader-datatype]
                  :as access-map} casting/buffer-access-table]
             [access-map
              `(fn [buffer# unchecked?#]
                 (let [buffer# (typecast/datatype->list-cast-fn
                                ~buffer-datatype buffer#)]
                   (make-mutable
                    ~(typecast/datatype->mutable-type reader-datatype)
                    ~(typecast/datatype->list-type buffer-datatype)
                    buffer#
                    ~reader-datatype
                    ~intermediate-datatype
                    ~buffer-datatype
                    unchecked?#)))])]
        (into {})))


(def list-mutable-table (make-list-mutable-table))


(defn make-list-mutable
  [item
   mutable-datatype
   intermediate-datatype
   & [unchecked?]]
  (let [list-buffer (dtype-proto/->list-backing-store item)
        buffer-dtype (dtype-proto/get-datatype list-buffer)
        access-map {:reader-datatype mutable-datatype
                    :intermediate-datatype intermediate-datatype
                    :buffer-datatype buffer-dtype}
        mut-fn (get list-mutable-table access-map)]
    (when-not mut-fn
      (throw (ex-info "Failed to find mutable operator for list type:"
                      access-map)))
    (mut-fn list-buffer unchecked?)))


(defmacro reify-marshalling-mutable
  [src-dtype dst-dtype]
  `(fn [buffer# datatype# unchecked?#]
     (let [buffer# (typecast/datatype->mutable
                    ~dst-dtype buffer# unchecked?#)]
       (if unchecked?#
         (reify ~(typecast/datatype->mutable-type src-dtype)
           (getDatatype [item#] datatype#)
           (lsize [mut-item#] (.lsize buffer#))
           (insert [item# idx# value#]
             (.insert buffer# idx#
                      (casting/datatype->unchecked-cast-fn
                       ~src-dtype ~dst-dtype value#)))
           (append [mut-item# value#]
             (.append buffer#
                      (casting/datatype->unchecked-cast-fn
                       ~src-dtype ~dst-dtype value#)))
           (mremove [item# idx#]
             (.mremove buffer# idx#)))
         (reify ~(typecast/datatype->mutable-type src-dtype)
           (getDatatype [item#] datatype#)
           (lsize [mut-item#] (.lsize buffer#))
           (insert [item# idx# value#]
             (.insert buffer# idx#
                      (casting/datatype->cast-fn
                       ~src-dtype ~dst-dtype value#)))
           (append [mut-item# value#]
             (.append buffer#
                      (casting/datatype->cast-fn ~src-dtype ~dst-dtype value#)))
           (mremove [item# idx#]
             (.mremove buffer# idx#)))))))


(def marshalling-mutable-table (casting/make-marshalling-item-table
                                reify-marshalling-mutable))


(defmacro extend-mutable
  [datatype]
  `(clojure.core/extend
       ~(typecast/datatype->mutable-type datatype)
     dtype-proto/PToMutable
     {:convertible-to-mutable? (constantly true)
      :->mutable
      (fn [item# options#]
        (let [mut-dtype# (or (:datatype options#) (dtype-proto/get-datatype item#))]
          (if (= mut-dtype# (dtype-proto/get-datatype item#))
            item#
            (if-let [mutable-fn# (get marshalling-mutable-table
                                      [(casting/safe-flatten mut-dtype#) ~datatype])]
              (do
                (mutable-fn# item# mut-dtype# (:unchecked? options#)))
              (throw (ex-info (format "Failed to find marshalling mutable: %s %s"
                                      ~datatype mut-dtype#)
                              {:src-datatype ~datatype
                               :dst-datatype mut-dtype#}))))))}))

(extend-mutable :int8)
(extend-mutable :int16)
(extend-mutable :int32)
(extend-mutable :int64)
(extend-mutable :float32)
(extend-mutable :float64)
(extend-mutable :boolean)
(extend-mutable :object)
