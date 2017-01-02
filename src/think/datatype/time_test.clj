(ns think.datatype.time-test
  (:require [think.datatype.core :refer :all])
  (:import [java.nio DoubleBuffer]))


(defn time-test
  [op]
  (dotimes [iter 10]
    (op))
  (time
   (dotimes [iter 100]
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
  (nio-buffer-fast-path-time-test))
