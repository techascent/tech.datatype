(ns tech.v2.datatype.simple-ops-perf-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.comparator :as comp]
            [tech.parallel.for :as parallel-for]
            [criterium.core :as crit])
  (:import [it.unimi.dsi.fastutil.ints IntArrays IntComparator]
           [java.util Comparator Arrays]))


(set! *unchecked-math* :warn-on-boxed)


(def n-elems 1000000)
(def src-data (range n-elems))
(def src-ary (long-array src-data))
(def src-rdr (typecast/datatype->reader :int64 src-data))
(def dst-data (long-array n-elems))


(defn hardcode-copy
  []
  (parallel-for/parallel-for
   idx
   (long n-elems)
   (aset ^longs dst-data idx (unchecked-multiply 2 (aget ^longs src-ary idx)))))


(defn reader-copy
  [rdr]
  (let [rdr (typecast/datatype->reader :int64 rdr)]
    (parallel-for/parallel-for
     idx
     (long n-elems)
     (aset ^longs dst-data idx (.read rdr idx)))))


(defn reader-writer-copy
  [rdr wtr]
  (let [rdr (typecast/datatype->reader :int64 rdr)
        wtr (typecast/datatype->writer :int64 wtr)]
    (parallel-for/parallel-for
     idx
     (long n-elems)
     (.write wtr
             idx (.read rdr idx)))))

(def obj-data (object-array (shuffle (range n-elems))))

(defn arrays-comparator-sort
  [ary-data]
  (let [test-comp (comparator <)]
    (Arrays/sort ary-data test-comp)
    (dtype/make-container :java-array :int64 (dtype/->reader ary-data :int64))))


(def int-data (int-array (shuffle (range n-elems))))

(defn int-arrays-quicksort
  []
  (let [test-comp (comp/make-comparator
                   :int32 (- lhs rhs))]
    (IntArrays/quickSort int-data test-comp)))


(defn parallel-int-arrays-quicksort
  []
  (let [test-comp (comp/make-comparator
                   :int32 (- lhs rhs))]
    (IntArrays/parallelQuickSort int-data test-comp)))
