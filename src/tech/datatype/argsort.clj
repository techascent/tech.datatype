(ns tech.datatype.argsort
  (:require [tech.datatype.typecast :as typecast]
            [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.comparator :refer :all]
            [clojure.core.matrix.protocols :as mp])
  (:import [it.unimi.dsi.fastutil.bytes ByteArrays ByteComparator]
           [it.unimi.dsi.fastutil.shorts ShortArrays ShortComparator]
           [it.unimi.dsi.fastutil.ints IntArrays IntComparator]
           [it.unimi.dsi.fastutil.longs LongArrays LongComparator]
           [it.unimi.dsi.fastutil.floats FloatArrays FloatComparator]
           [it.unimi.dsi.fastutil.doubles DoubleArrays DoubleComparator]
   [tech.datatype
            Comparator$ByteComp
            Comparator$ShortComp
            Comparator$IntComp
            Comparator$LongComp
            Comparator$FloatComp
    Comparator$DoubleComp]
   [java.util Comparator]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro make-argsort
  [datatype]
  `(fn [values# parallel?# reverse?# comparator#]
     (let [comparator# (or comparator#
                           (make-comparator ~datatype
                                            (default-compare-fn ~datatype ~'lhs ~'rhs)))
           n-elems# (int (mp/element-count values#))]
       (if (= n-elems# 0)
         (int-array 0)
         (let [index-array# (int-array (range n-elems#))
               values# (typecast/datatype->reader ~datatype values# true)
               value-comparator# (datatype->comparator ~datatype comparator#)
               idx-comparator# (if reverse?#
                                 (make-comparator
                                  :int32 (.compare value-comparator#
                                                   (.read values# ~'rhs)
                                                   (.read values# ~'lhs)))
                                 (make-comparator
                                  :int32 (.compare value-comparator#
                                                   (.read values# ~'lhs)
                                                   (.read values# ~'rhs))))]

           (if parallel?#
             (IntArrays/parallelQuickSort index-array# (int32-comparator idx-comparator#))
             (IntArrays/quickSort index-array# (int32-comparator idx-comparator#)))
           index-array#)))))


(defmacro make-argsort-table
  []
  `(->> [~@(for [dtype (concat casting/host-numeric-types
                               [:object])]
             [dtype `(make-argsort ~dtype)])]
        (into {})))


(def argsort-table (make-argsort-table))


(defn argsort
  [values {:keys [parallel?
                  typed-comparator
                  datatype
                  reverse?]
           :or {parallel? true}}]
  (let [datatype (or datatype (dtype-base/get-datatype values))
        sort-fn (get argsort-table (-> datatype
                                       casting/flatten-datatype
                                       casting/datatype->safe-host-type))]
    (sort-fn values parallel? reverse? typed-comparator)))
