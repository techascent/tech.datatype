(ns tech.v2.datatype.writers.concat
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.typecast :as typecast])
  (:import [java.util List]))



(defmacro make-concat-writer-impl
  [datatype]
  `(fn [datatype# concat-args#]
     (let [^List writer-args# (mapv #(dtype-proto/->writer % {:datatype datatype#})
                                    concat-args#)
           total-size# (long (apply + (map #(dtype-base/ecount %) writer-args#)))]
       (reify
         dtype-proto/PDatatype
         (get-datatype [item#] datatype#)
         ~(typecast/datatype->writer-type datatype)
         (lsize [rdr#] total-size#)
         (write [rdr# idx# value#]
           (loop [rdr-idx# 0
                  idx# idx#]
             (let [cur-rdr# (typecast/datatype->writer ~datatype
                                                       (.get writer-args# rdr-idx#))
                   cur-lsize# (.lsize cur-rdr#)]
               (if (< idx# cur-lsize#)
                 (.write cur-rdr# idx# value#)
                 (recur (inc rdr-idx#) (- idx# cur-lsize#))))))))))


(def concat-writer-table (casting/make-base-datatype-table make-concat-writer-impl))


(defn concat-writers
  ([options writers]
   (let [datatype (or (:datatype options) (dtype-base/get-datatype (first writers)))
         writer-fn (get concat-writer-table (casting/safe-flatten datatype))]
     (writer-fn datatype writers)))
  ([writers]
   (concat-writers {} writers)))
