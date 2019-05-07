(ns tech.v2.datatype.writers.iterable-to-writer
    (:require [tech.v2.datatype.casting :as casting]
              [tech.v2.datatype.protocols :as dtype-proto]
              [tech.v2.datatype.typecast :as typecast]))


(defmacro make-iterable-write-fn
  [datatype]
  `(fn [dst# src# unchecked?#]
     (let [dst-writer# (typecast/datatype->writer ~datatype dst# true)
           src-iter# (typecast/datatype->iter ~datatype src# unchecked?#)]
       (loop [idx# (int 0)]
         (if (.hasNext src-iter#)
           (do
             (.write dst-writer# idx# (typecast/datatype->iter-next-fn
                                       ~datatype src-iter#))
             (recur (unchecked-inc idx#)))
           dst#)))))


(def iterable-writer-table (casting/make-base-datatype-table
                            make-iterable-write-fn))


(defn iterable->writer
  [dst-writer src-iterable & {:keys [datatype unchecked?]}]
  (let [datatype (or datatype (dtype-proto/get-datatype src-iterable))
        writer-fn (get iterable-writer-table (casting/safe-flatten datatype))]
    (writer-fn dst-writer src-iterable unchecked?)))
