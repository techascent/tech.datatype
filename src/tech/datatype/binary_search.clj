(ns tech.datatype.binary-search
  (:require [tech.datatype.typecast :as typecast]
            [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.comparator :as dtype-comp]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro make-binary-search
  [datatype]
  `(fn [values# target# comparator#]
     (let [target# (casting/datatype->cast-fn :unknown ~datatype target#)
           values# (typecast/datatype->reader ~datatype values# true)
           comparator# (or comparator#
                           (dtype-comp/make-comparator
                            ~datatype (dtype-comp/default-compare-fn
                                       ~datatype ~'lhs ~'rhs)))
           comparator# (dtype-comp/datatype->comparator ~datatype comparator#)
           buf-ecount# (.size values#)]
       (if (= 0 buf-ecount#)
         [false 0]
         (loop [low# (int 0)
                high# (int buf-ecount#)]
           (if (< low# high#)
             (let [mid# (+ low# (quot (- high# low#) 2))
                   buf-data# (.read values# mid#)
                   compare-result# (.compare comparator# buf-data# target#)]
               (if (= 0 compare-result#)
                 (recur mid# mid#)
                 (if (and (< compare-result# 0)
                          (not= mid# low#))
                   (recur mid# high#)
                   (recur low# mid#))))
             (let [buf-val# (.read values# low#)]
               (if (<= (.compare comparator# target# buf-val#) 0)
                 [(= target# buf-val#) low#]
                 [false (unchecked-inc low#)]))))))))


(defmacro make-binary-search-table
  []
  `(->> [~@(for [dtype casting/host-numeric-types]
             [dtype `(make-binary-search ~dtype)])]
        (into {})))

(def binary-search-table (make-binary-search-table))


(defn binary-search
  "Perform binary search returning long idx of matching value or insert position.
  Returns index of the element or the index where it should be inserted.  Returns
  a tuple of [found? insert-or-elem-pos]"
  [values target {:keys [datatype
                         comparator]}]
  (let [datatype (or datatype (dtype-base/get-datatype target))]
    (if-let [value-fn (get binary-search-table (casting/datatype->safe-host-type
                                                datatype))]
      (value-fn values target comparator)
      (throw (ex-info (format "No search mechanism found for datatype %s" datatype)
                      {})))))
