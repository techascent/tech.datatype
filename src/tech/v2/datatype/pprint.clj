(ns tech.v2.datatype.pprint
  (:require [tech.v2.datatype.protocols :as dtype-proto]))


;; pretty-printing utilities for matrices
(def ^:dynamic *number-format* "%.3f")


(defn format-num [x]
  (if (integer? x)
    (str x)
    (format *number-format* (double x))))


(defn format-object
  [x]
  (if (number? x)
    (format-num x)
    (str x)))


(defn print-reader-data
  [rdr & {:keys [formatter]
          :or {formatter format-object}}]
  (->> (dtype-proto/->reader rdr {})
       (reduce (fn [^StringBuilder builder val]
                 (.append builder
                          (formatter val))
                 (.append builder ", "))
               (StringBuilder.))
       (.toString)))
