(ns tech.v2.datatype.argsort
  (:require [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.comparator :as dtype-comp])
  (:import [it.unimi.dsi.fastutil.bytes ByteArrays ByteComparator]
           [it.unimi.dsi.fastutil.shorts ShortArrays ShortComparator]
           [it.unimi.dsi.fastutil.ints IntArrays IntComparator]
           [it.unimi.dsi.fastutil.longs LongArrays LongComparator]
           [it.unimi.dsi.fastutil.floats FloatArrays FloatComparator]
           [it.unimi.dsi.fastutil.doubles DoubleArrays DoubleComparator]
   [tech.v2.datatype
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
                           (dtype-comp/default-comparator ~datatype))
           n-elems# (int (dtype-proto/ecount values#))]
       (if (= n-elems# 0)
         (int-array 0)
         (let [index-array# (int-array (range n-elems#))
               values# (typecast/datatype->reader ~datatype values# true)
               value-comparator# (dtype-comp/datatype->comparator ~datatype comparator#)
               idx-comparator# (if reverse?#
                                 (dtype-comp/make-comparator
                                  :int32 (.compare value-comparator#
                                                   (.read values# ~'rhs)
                                                   (.read values# ~'lhs)))
                                 (dtype-comp/make-comparator
                                  :int32 (.compare value-comparator#
                                                   (.read values# ~'lhs)
                                                   (.read values# ~'rhs))))]

           (if parallel?#
             (IntArrays/parallelQuickSort index-array# (dtype-comp/int32-comparator
                                                        idx-comparator#))
             (IntArrays/quickSort index-array# (dtype-comp/int32-comparator
                                                idx-comparator#)))
           index-array#)))))


(def argsort-table (casting/make-base-no-boolean-datatype-table
                    make-argsort))


(defn argsort
  [values {:keys [parallel?
                  typed-comparator
                  datatype
                  reverse?]
           :or {parallel? true}}]
  (let [datatype (or datatype (dtype-base/get-datatype values))
        sort-fn (get argsort-table (casting/safe-flatten datatype))]
    (sort-fn values parallel? reverse? typed-comparator)))
