(ns tech.v2.datatype.readers.indexed
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]))


(defmacro make-int64-indexed-reader-impl
  [datatype]
  `(fn [indexes# values# unchecked?#]
     (let [idx-reader# (typecast/datatype->reader :int64 indexes# true)
           values-dtype# (dtype-proto/get-datatype values#)
           values# (typecast/datatype->reader ~datatype values# unchecked?#)
           n-elems# (.lsize idx-reader#)]
       (reify ~(typecast/datatype->reader-type datatype)
         (getDatatype [item#] values-dtype#)
         (lsize [item#] (.lsize idx-reader#))
         (read [item# idx#]
           (.read values# (.read idx-reader# idx#)))
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [item#]
           (dtype-proto/has-constant-time-min-max? values#))
         (constant-time-min [item#]
           (dtype-proto/constant-time-min values#))
         (constant-time-max [item#]
           (dtype-proto/constant-time-max values#))
         dtype-proto/PToBackingStore
         (->backing-store-seq [item]
           (concat (dtype-proto/->backing-store-seq idx-reader#)
                   (dtype-proto/->backing-store-seq values#)))))))


(def int64-indexed-reader-creators (casting/make-base-datatype-table
                                    make-int64-indexed-reader-impl))


(defmacro make-int32-indexed-reader-impl
  [datatype]
  `(fn [indexes# values# unchecked?#]
     (let [idx-reader# (typecast/datatype->reader :int32 indexes# true)
           values-dtype# (dtype-proto/get-datatype values#)
           values# (typecast/datatype->reader ~datatype values# unchecked?#)
           n-elems# (.lsize idx-reader#)]
       (reify ~(typecast/datatype->reader-type datatype)
         (getDatatype [item#] values-dtype#)
         (lsize [item#] (.lsize idx-reader#))
         (read [item# idx#]
           (.read values# (.read idx-reader# idx#)))
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [item#]
           (dtype-proto/has-constant-time-min-max? values#))
         (constant-time-min [item#]
           (dtype-proto/constant-time-min values#))
         (constant-time-max [item#]
           (dtype-proto/constant-time-max values#))
         dtype-proto/PToBackingStore
         (->backing-store-seq [item]
           (concat (dtype-proto/->backing-store-seq idx-reader#)
                   (dtype-proto/->backing-store-seq values#)))))))


(def int32-indexed-reader-creators (casting/make-base-datatype-table
                                    make-int32-indexed-reader-impl))


(defn make-indexed-reader
  ([indexes values options]
   (let [values (dtype-proto/->reader values options)
         datatype (dtype-proto/get-datatype values)
         reader-fn (if (= :int32 (dtype-proto/get-datatype indexes))
                     (get int32-indexed-reader-creators
                          (casting/safe-flatten datatype))
                     (get int64-indexed-reader-creators
                          (casting/safe-flatten datatype)))]
     (reader-fn indexes values (:unchecked? options))))
  ([indexes values]
   (make-indexed-reader indexes values {})))


;;Maybe values is random-read but the indexes are a large sequence
;;In this case we need the indexes to be an iterator.
(defmacro make-indexed-iterable
  [datatype]
  `(fn [indexes# values# unchecked?#]
     (let [values# (typecast/datatype->reader ~datatype values# unchecked?#)]
        (reify
          Iterable
          (iterator [item#]
            (let [idx-iter# (typecast/datatype->iter :int32 indexes# true)]
              (reify ~(typecast/datatype->iter-type datatype)
                (getDatatype [item#] ~datatype)
                (hasNext [item#] (.hasNext idx-iter#))
                (~(typecast/datatype->iter-next-fn-name datatype)
                 [item#]
                 (let [next-idx# (.nextInt idx-iter#)]
                   (.read values# next-idx#)))
                (current [item#]
                  (.read values# (.current idx-iter#))))))
          dtype-proto/PDatatype
          (get-datatype [item#] ~datatype)))))


(def indexed-iterable-table (casting/make-base-datatype-table make-indexed-iterable))


(defn make-iterable-indexed-iterable
  ([indexes values {:keys [datatype unchecked?]}]
   (let [datatype (or datatype (dtype-proto/get-datatype values))
         reader-fn (get indexed-iterable-table (casting/safe-flatten datatype))]
     (reader-fn indexes values unchecked?)))
  ([indexes values]
   (make-iterable-indexed-iterable indexes values {})))
