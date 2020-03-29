(ns tech.v2.datatype.pprint
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.argtypes :as argtypes]))


;; pretty-printing utilities for matrices
(def ^:dynamic *number-format* "%.4G")


(defn format-num [x]
  (if (integer? x)
    (str x)
    (format *number-format* (double x))))


(defn format-object
  [x]
  (if (number? x)
    (format-num x)
    (str x)))


(defmulti reader-printer
  (fn [item]
    (when item
      (dtype-proto/get-datatype item))))


(defmethod reader-printer :default
  [item]
  (let [argtype (argtypes/arg->arg-type item)]
    (if (= argtype :reader)
      (dtype-proto/->reader item {})
      item)))


(defn print-reader-data
  [rdr & {:keys [formatter]
          :or {formatter format-object}}]
  (->> (reader-printer rdr)
       (reduce (fn [^StringBuilder builder val]
                 (.append builder
                          (formatter val))
                 (.append builder ", "))
               (StringBuilder.))
       (.toString)))
