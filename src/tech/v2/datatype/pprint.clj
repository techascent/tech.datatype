(ns tech.v2.datatype.pprint
  (:require [tech.v2.datatype.protocols :as dtype-proto]))


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


(defmulti reader-converter
  "Given a item that is of a datatype that is unprintable or that prints incorrectly
  return a new reader of a datatype that will print correctly (or just a reader of
  strings is fine).  This is sometimes called for iterables also."
  (fn [item]
    (when item
      (dtype-proto/get-datatype item))))


(defmethod reader-converter :default
  [item]
  item)


(defn print-reader-data
  [rdr & {:keys [formatter]
          :or {formatter format-object}}]
  (let [rdr (reader-converter rdr)
        ^StringBuilder builder
        (->> (dtype-proto/->reader rdr {})
             (reduce (fn [^StringBuilder builder val]
                       (.append builder
                                (formatter val))
                       (.append builder ", "))
                     (StringBuilder.)))]
    (.toString builder)))
