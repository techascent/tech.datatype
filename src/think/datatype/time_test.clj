(ns think.datatype.time-test
  (:require [think.datatype.core :refer :all]
            [clojure.core.matrix.macros :refer [c-for]]
            [think.datatype.marshal :as marshal])
  (:import [java.nio DoubleBuffer]
           [think.datatype DoubleArrayView FloatArrayView
            LongArrayView IntArrayView ShortArrayView ByteArrayView
            ArrayView]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn time-test
  [op]
  (dotimes [iter 100]
    (op))
  (time
   (dotimes [iter 500]
     (op))))

(def n-elems 100000)


(defn generic-copy-time-test
  []
  (let [src-buffer (float-array (range n-elems))
        dest-buffer (double-array n-elems)]
    (time-test
     #(generic-copy! src-buffer 0 dest-buffer 0 n-elems))))


(defn datatype-copy-time-test
  []
  (let [src-buffer (float-array (range n-elems))
        dest-buffer (double-array n-elems)]
    (time-test
     #(copy! src-buffer 0 dest-buffer 0 n-elems))))


;;Make sure we didn't screw of copies using the fast path of the nio buffer
(defn nio-buffer-fast-path-time-test
  []
  (let [src-buffer (double-array (range n-elems))
        dest-buffer (DoubleBuffer/wrap (double-array n-elems))]
    (time-test
     #(copy! src-buffer 0 dest-buffer 0 n-elems))))


(defn nio-buffer-marshal-slow-path-time-test
  []
  (let [src-buffer (float-array (range n-elems))
        dest-buffer (DoubleBuffer/wrap (double-array n-elems))]
    (time-test
     #(generic-copy! src-buffer 0 dest-buffer 0 n-elems))))


(defn nio-buffer-marshal-fast-path-time-test
  []
  (let [src-buffer (float-array (range n-elems))
        dest-buffer (DoubleBuffer/wrap (double-array n-elems))]
    (time-test
     #(copy! src-buffer 0 dest-buffer 0 n-elems))))


(defn array-view-copy-time-test
  []
  (let [src-buffer (->view (float-array (range (+ (long n-elems) 10))) 10 n-elems)
        dest-buffer (double-array n-elems)]
    (time-test
     #(copy! src-buffer 0 dest-buffer 0 n-elems))))


(defn array-into-view-time-test
  []
  (let [src-buffer (float-array (range (+ (long n-elems) 10)))
        dest-buffer (->view (double-array (+ (long n-elems) 10)) 10 n-elems)]
    (time-test
     #(copy! src-buffer 0 dest-buffer 0 n-elems))))


(defn run-time-tests
  []
  (println "float array -> double array generic")
  (generic-copy-time-test)
  (println "float array -> double array fast path")
  (datatype-copy-time-test)
  (println "float array -> double nio buffer slow path")
  (nio-buffer-marshal-slow-path-time-test)
  (println "float array -> double nio buffer fast path")
  (nio-buffer-marshal-fast-path-time-test)
  (println "double array -> double nio buffer fast path")
  (nio-buffer-fast-path-time-test)
  (println "float array view -> double fast path")
  (array-view-copy-time-test)
  (println "float array -> double array view fast path")
  (array-into-view-time-test))


(defn indexed-copy-time-test
  [src-fn dest-fn]
  (let [src-data (src-fn (range n-elems))
        src-indexes (int-array (range n-elems))
        dest-indexes (int-array (reverse (range n-elems)))
        dest-data (dest-fn n-elems)
        src-offset (long 0)
        dest-offset (long 0)]
    (time-test
     #(indexed-copy! src-data src-offset src-indexes dest-data dest-offset dest-indexes))))


(defn indexed-copy-array-manual-test
  []
  (let [^floats src-data (make-array-of-type :float (range n-elems))
        src-offset (long 0)
        dest-offset (long 0)
        src-indexes (int-array (range n-elems))
        dest-indexes (int-array (reverse (range n-elems)))
        ^doubles dest-data (make-array-of-type :double n-elems)
        n-elems (long n-elems)]
    (time-test
     (fn []
       (c-for [idx 0 (< idx n-elems) (inc idx)]
              (aset dest-data (+ dest-offset (aget dest-indexes idx))
                    (float (aget src-data (+ src-offset
                                             (aget src-indexes idx))))))))))


(defn indexed-copy-array-no-offset-manual-test
  []
  (let [^floats src-data (make-array-of-type :float (range n-elems))
        src-indexes (int-array (range n-elems))
        dest-indexes (int-array (reverse (range n-elems)))
        ^doubles dest-data (make-array-of-type :double n-elems)
        n-elems (long n-elems)]
    (time-test
     (fn []
       (c-for [idx 0 (< idx n-elems) (inc idx)]
              (aset dest-data (aget dest-indexes idx)
                    (float (aget src-data (aget src-indexes idx)))))))))


(defn indexed-copy-generate-test
  []
  (let [^floats src-data (make-array-of-type :float (range n-elems))
        src-offset (long 0)
        dest-offset (long 0)
        src-indexes (int-array (range n-elems))
        dest-indexes (int-array (reverse (range n-elems)))
        ^doubles dest-data (make-array-of-type :double n-elems)
        copy-fn (marshal/create-indexed-array->array-fn marshal/as-float-array
                                                        marshal/as-double-array
                                                        float)]
    (time-test #(copy-fn src-data src-offset src-indexes dest-data dest-offset dest-indexes))))

(defn indexed-copy-cached-test
  []
  (let [^floats src-data (make-array-of-type :float (range n-elems))
        src-offset (long 0)
        dest-offset (long 0)
        src-indexes (int-array (range n-elems))
        dest-indexes (int-array (reverse (range n-elems)))
        ^doubles dest-data (make-array-of-type :double n-elems)
        copy-fn (marshal/get-indexed-copy-to-fn dest-data dest-offset)]
    (println (type copy-fn))
    (time-test #(copy-fn src-data src-offset src-indexes dest-indexes))))


(defn indexed-copy-array-generic-test
  []
  (let [^floats src-data (make-array-of-type :float (range n-elems))
        src-offset (long 0)
        dest-offset (long 0)
        src-indexes (int-array (range n-elems))
        dest-indexes (int-array (reverse (range n-elems)))
        ^doubles dest-data (make-array-of-type :double n-elems)
        n-elems (long n-elems)]
    (time-test
     #(generic-indexed-copy! src-data src-offset src-indexes
                             dest-data dest-offset dest-indexes))))


(defn run-indexed-time-tests
  []
  (println "array->array-indexed-copy")
  (indexed-copy-time-test #(make-array-of-type :float %)
                          #(make-array-of-type :double %))
  (println "indexed-copy hand-coded")
  (indexed-copy-array-manual-test)
  (println "indexed-copy-no-offset-hand-coded")
  (indexed-copy-array-no-offset-manual-test)
  (println "indexed-copy generate")
  (indexed-copy-generate-test)
  (println "indexed-copy cached")
  (indexed-copy-cached-test)
  (println "indexed-copy-generic")
  (indexed-copy-array-generic-test)
  (println "array->array-indexed-copy")
  (indexed-copy-time-test #(make-array-of-type :float %)
                          #(make-array-of-type :double %)))
