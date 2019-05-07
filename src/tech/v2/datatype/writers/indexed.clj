(ns tech.v2.datatype.writers.indexed
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]))


(declare make-indexed-writer)



(defmacro make-indexed-writer-impl
  [datatype]
 `(fn [indexes# values# unchecked?#]
    (let [idx-reader# (typecast/datatype->reader :int32 indexes# true)
          values# (typecast/datatype->writer ~datatype values# unchecked?#)
          writer-dtype# (dtype-proto/get-datatype values#)
           n-elems# (.lsize idx-reader#)]
       (reify
         ~(typecast/datatype->writer-type datatype)
         (getDatatype [writer#] writer-dtype#)
         (lsize [writer#] n-elems#)
         (write [writer# idx# value#]
           (.write values# (.read idx-reader# idx#) value#))
         dtype-proto/PToBackingStore
         (->backing-store-seq [writer#]
           (dtype-proto/->backing-store-seq values#))
         dtype-proto/PBuffer
         (sub-buffer [writer# offset# length#]
           (-> (dtype-proto/sub-buffer indexes# offset# length#)
               (make-indexed-writer values# {:unchecked? unchecked?#})))))))


(def indexed-writer-creators (casting/make-base-datatype-table
                              make-indexed-writer-impl))


(defn make-indexed-writer
  [indexes values {:keys [datatype unchecked?]}]
  (let [datatype (or datatype (dtype-proto/get-datatype values))
        writer-fn (get indexed-writer-creators (casting/flatten-datatype datatype))]
    (writer-fn indexes values unchecked?)))
