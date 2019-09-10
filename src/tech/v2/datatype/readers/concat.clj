(ns tech.v2.datatype.readers.concat
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.typecast :as typecast])
  (:import [java.util List]))



(defmacro make-concat-reader-impl
  [datatype]
  `(fn [datatype# concat-args#]
     (let [^List reader-args# (mapv #(dtype-proto/->reader % {:datatype datatype#})
                                    concat-args#)
           total-size# (long (apply + (map #(dtype-base/ecount %) reader-args#)))]
       (reify
         dtype-proto/PDatatype
         (get-datatype [item#] datatype#)
         ~(typecast/datatype->reader-type datatype)
         (lsize [rdr#] total-size#)
         (read [rdr# idx#]
           (loop [rdr-idx# 0
                  idx# idx#]
             (let [cur-rdr# (typecast/datatype->reader ~datatype
                                                       (.get reader-args# rdr-idx#))
                   cur-lsize# (.lsize cur-rdr#)]
               (if (< idx# cur-lsize#)
                 (.read cur-rdr# idx#)
                 (recur (inc rdr-idx#) (- idx# cur-lsize#))))))))))


(def concat-reader-table (casting/make-base-datatype-table make-concat-reader-impl))


(defn concat-readers
  ([options readers]
   (let [datatype (or (:datatype options) (dtype-base/get-datatype (first readers)))
         reader-fn (get concat-reader-table (casting/safe-flatten datatype))]
     (reader-fn datatype readers)))
  ([readers]
   (concat-readers {} readers)))
