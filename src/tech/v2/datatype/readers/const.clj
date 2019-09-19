(ns tech.v2.datatype.readers.const
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]))


(defmacro make-const-reader-macro
  [datatype]
  `(fn [item# num-elems#]
     (let [num-elems# (int (or num-elems# Integer/MAX_VALUE))
           item# (casting/datatype->cast-fn
                  :unknown ~datatype item#)]
       (reify ~(typecast/datatype->reader-type datatype)
         (getDatatype [reader#] ~datatype)
         (lsize [reader#] num-elems#)
         (read [reader# idx#] item#)))))


(def const-reader-table (casting/make-base-datatype-table
                         make-const-reader-macro))


(defn make-const-reader
  [item datatype & [num-elems]]
  (if-let [reader-fn (get const-reader-table (casting/flatten-datatype datatype))]
    (reader-fn item num-elems)
    (throw (ex-info (format "Failed to find reader for datatype %s" datatype) {}))))
