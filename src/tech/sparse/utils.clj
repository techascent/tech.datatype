(ns tech.sparse.utils
  (:require [tech.datatype.typecast :as typecast]
            [tech.datatype.casting :as casting]
            [tech.datatype :as dtype]
            [tech.datatype.unary-op :as unary-op]
            [tech.datatype.binary-search :as dtype-search])
  (:import [tech.datatype UnaryOperators$IntUnary]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)



(defmacro ^:private make-sparse-copier
  [datatype]
  `(fn [lhs-buffer# rhs-buffer# index-seq#]
     (let [lhs-buffer# (typecast/datatype->reader ~datatype lhs-buffer#)
           rhs-buffer# (typecast/datatype->writer ~datatype rhs-buffer#)]
       (doseq [{:keys [~'data-index ~'global-index]} index-seq#]
         (.write rhs-buffer# (int ~'global-index)
                 (.read lhs-buffer# (int ~'data-index)))))
     rhs-buffer#))


(defmacro make-sparse-copier-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             [dtype `(make-sparse-copier ~dtype)])]
        (into {})))


(def sparse-copier-table (make-sparse-copier-table))


(defn typed-sparse-copy!
  [src dest index-seq]
  (if-let [copier-fn (get sparse-copier-table
                          (-> (dtype/get-datatype dest)
                              casting/flatten-datatype))]
    (copier-fn src dest index-seq)
    (throw (ex-info (format "Failed to find copier for datatype: %s"
                            (dtype/get-datatype dest))))))
