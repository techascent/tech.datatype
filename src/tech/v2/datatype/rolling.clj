(ns tech.v2.datatype.rolling
  (:require [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.binary-op :as binary-op]
            [tech.v2.datatype.reduce-op :as reduce-op]
            [tech.v2.datatype.argtypes :as argtypes]
            [tech.v2.datatype.casting :as casting]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn do-pad-last
  [n-pad item-seq advanced-item-seq]
  (let [n-pad (long n-pad)]
    (if (seq advanced-item-seq)
      (cons (first item-seq)
            (lazy-seq (do-pad-last n-pad (rest item-seq)
                                   (rest advanced-item-seq))))
      (when (>= n-pad 0)
        (cons (first item-seq)
              (lazy-seq (do-pad-last (dec n-pad) item-seq nil)))))))


(defn pad-last
  [n-pad item-seq]
  (do-pad-last n-pad item-seq (rest item-seq)))


(defn pad-sequence
  "Repeat the first and last members of the sequence pad times"
  [n-pad item-seq]
  (concat (repeat n-pad (first item-seq))
          (pad-last n-pad item-seq)))


(defn fixed-window-sequence
  "Return a sequence of fixed windows.  Stops when the next window cannot
  be fulfilled."
  [window-size n-skip item-sequence]
  (let [window-size (long window-size)
        next-window (vec (take window-size item-sequence))]
    (when (= window-size (count next-window))
      (cons next-window (lazy-seq (fixed-window-sequence
                                   window-size n-skip
                                   (drop n-skip item-sequence)))))))


(defmacro make-padded-reader
  [datatype runtime-dtype window-size
   src-reader n-pad last-index
   offset]
  `(reify ~(typecast/datatype->reader-type datatype)
     (getDatatype [item#] ~runtime-dtype)
     (lsize [item#] ~window-size)
     (read [item# idx#]
       (.read ~src-reader (-> (- (+ idx# ~offset) ~n-pad)
                              (max 0)
                              (min ~last-index))))))


(defmacro make-window-reader-macro
  [datatype]
  `(fn [window-size# runtime-dtype# reader# window-fn# n-pad#]
     (let [reader# (typecast/datatype->reader ~datatype reader#)
           window-size# (long window-size#)
           n-pad# (long n-pad#)
           n-elems# (.lsize reader#)
           last-idx# (- n-elems# 1)]
       (reify ~(typecast/datatype->reader-type datatype)
         (getDatatype [item#] runtime-dtype#)
         (lsize [item#] n-elems#)
         (read [item# idx#]
           (window-fn# (make-padded-reader ~datatype runtime-dtype# window-size#
                                           reader# n-pad# last-idx#
                                           idx#)))))))


(defmacro make-window-reader-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-window-reader-macro ~dtype)])]
        (into {})))


(def window-reader-table (make-window-reader-table))



(defn fixed-rolling-window
  "Return a lazily evaluated rolling window of window-fn applied to each window.  The
  iterable or sequence is padded such that there are the same number of values in the
  result as in the input.
  If input is an iterator, output is an lazy sequence.  If input is a reader,
  output is a reader."
  [window-size window-fn item]
  (let [n-pad (quot (long window-size) 2)]
    (case (argtypes/arg->arg-type item)
      :scalar (throw (ex-info "Rolling windows aren't defined on scalars" {}))
      :iterable (->> (pad-sequence n-pad item)
                     (fixed-window-sequence window-size 1)
                     (pmap window-fn))
      :reader ((get window-reader-table (casting/safe-flatten
                                         (dtype-base/get-datatype item)))
               window-size (dtype-base/get-datatype item) item window-fn n-pad))))


(defn iterator-time-test
  "How much does boxing/iterating actually cost?
  - time this function a few times."
  []
  (let [ary-data (double-array (range 100000))
        iter-reduce-fn #(apply min %1)]
    (->> (fixed-rolling-window 20 iter-reduce-fn (seq ary-data))
         doall
         (take 20))))


(defn reader-time-test
  "How much does boxing/iterating actually cost?
  - time this function after timing iterator-time-test."
  []
  (let [ary-data (double-array (range 100000))
        result (double-array 100000)
        bin-reduce-fn (binary-op/datatype->binary-op
                       :float64 (:min binary-op/builtin-binary-ops)
                       true)
        reduce-fn (get reduce-op/iterable-reduce-table :float64)]
    ;;The copy is just to force the entire operation.
    (dtype-base/copy! (fixed-rolling-window
                       20 #(reduce-fn bin-reduce-fn %1 true)
                       ary-data)
                      result)
    (take 20 result)))
