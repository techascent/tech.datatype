(ns tech.v2.datatype.simple-ops-perf-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.functional :as dfn]
            [tech.parallel.for :as parallel-for]
            [criterium.core :as crit]))


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
