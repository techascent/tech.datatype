(ns think.datatype.transpose
  "Generalized transpose operation.  Given the dimensions of the src data
and the relative transpose indexes produce a new item that has the reordered
information.  When there are more than two dimensions the transpose operation is ambiguous
and many permutations are possible.

This algorithm could stand for review.  The general idea was to convert each input index
into relative dimension indexes (using combinations of mod and rem and stride).
Reorder the relative dimension indexes using the relative reorder vector and then convert back
to an output index using the output strides producing a 1:1 mapping from input index to output index.

One thing to note is that this file is written in little-endian format meaning index 0 holds the smallest thing
but we expect the initial dimension argument to be in big-endian as that matches core matrix conventions as well
as hdf5 conventions."
  (:require [think.datatype.core :as dtype]
            [clojure.core.matrix.macros :refer [c-for]]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn- dims->strides
  "Produce strides in little endian format."
  ^ints [dims]
  (-> (reduce (fn [retval next-dim]
                (let [last-stride (or (first retval) 1)
                      next-dim (or next-dim 1)]
                  (conj retval (* (long last-stride) (long next-dim)))))
              ()
              (reverse dims))
      (concat [1])
      (#(drop 1 %))
      reverse
      int-array))


(defn- le-dims-idx->dim-indexes!
  "Given dimensions in litte endian format and an index produce
  an new vector of relative indexes in each dimension of the item.
  The result is in little endian format."
  ^ints [^ints le-dims ^long idx ^ints retval]
  (let [num-iters (- (alength le-dims) 1)]
    (loop [leftover idx
           dim-idx 0]
      (if (< dim-idx num-iters)
        (let [dim (aget le-dims dim-idx)
              next-item (rem leftover dim)
              next-leftover (quot leftover dim)]
          (aset retval dim-idx next-item)
          (recur next-leftover (inc dim-idx)))
        (do
          (aset retval dim-idx (int leftover))
          retval)))))


(defn- le-dim-indexes-reshape->output-index
  "Create the output index from left-shifted output dims.  This means
that index 0 of output dims should hold a 1 and the entire vector should be shifted
by 1.  Reshape must be a vector ordered little endian from the output's perspective."
  ^long [^ints le-dim-idx ^ints reshape ^ints output-strides]
  (let [num-iters (alength output-strides)]
    (loop [retval 0
           idx 0]
      (if (< idx num-iters)
        (recur (+ retval (* (aget le-dim-idx (aget reshape idx))
                            (aget output-strides idx)))
               (inc idx))
        retval))))

(defn- input-idx->output-idx!
  "This is the actual workhorse of the whole method.  Given an input index convert to
input dimension indexes (an index per dimension) and then convert that to an output."
  [input-idx ^ints input-le-dims ^ints input-dim-indexes
   ^ints output-le-reshape ^ints output-le-strides]
  (->
   (le-dims-idx->dim-indexes! input-le-dims input-idx input-dim-indexes)
   (le-dim-indexes-reshape->output-index output-le-reshape output-le-strides)))


(defn transpose!
  "Given input with given dims and relative reshape indexes
produce a new array of double values in the order desired"
  [src data-dims reshape-indexes dest]
  (when-not (= (count (distinct reshape-indexes))
               (count data-dims))
    (throw (ex-info "Reshape array contains incorrect number of elements"
                    {:num-reshape-distinct (count (distinct reshape-indexes))
                     :num-dims (count data-dims)})))
  (when-not (every? #(and (>= (long %) 0)
                          (< (long %) (count data-dims)))
                    reshape-indexes)
    (throw (ex-info "Reshape indexes contains elements out of range (not indexes into data dims)"
                    {:reshape-indexes reshape-indexes
                     :min-index 0
                     :max-index (- (count data-dims) 1)})))
  (let [n-elems (long (reduce * data-dims))
        num-dims (long (dtype/ecount data-dims))
        input-le-dims (int-array (reverse data-dims))

        output-reshape (->> (reverse reshape-indexes) ;;Get into le format
                            (mapv #(- num-dims (long %) 1)) ;;from the output's perspective
                            int-array)
        output-le-strides (dims->strides (mapv data-dims reshape-indexes))
        input-dim-indexes (int-array (dtype/ecount data-dims))
        output-indexes (int-array n-elems)
        input-indexes (int-array (range n-elems))]
    (when-not (= (dtype/ecount src)
                 (dtype/ecount dest))
      (throw (ex-info "src and dest lengths do not match"
                      {:src-len (dtype/ecount src)
                       :dest-len (dtype/ecount dest)})))
    (when-not (= (dtype/ecount src)
                 n-elems)
      (throw (ex-info "src num elems does not match dimension num elems"
                      {:src-len (dtype/ecount src)
                       :data-dims (vec data-dims)
                       :data-dim-ecount n-elems})))
    (c-for [idx 0 (< idx n-elems) (inc idx)]
           (aset output-indexes idx (int (input-idx->output-idx! idx input-le-dims input-dim-indexes
                                                                 output-reshape output-le-strides))))
    (dtype/indexed-copy! src 0 input-indexes dest 0 output-indexes)
    dest))


(defn transpose
  "Transpose into an array of the given datatype returning the result."
  [src data-dims reshape-indexes & {:keys [datatype]
                                    :or {datatype :double}}]
  (transpose! src data-dims reshape-indexes
              (dtype/make-array-of-type datatype (long (apply * data-dims)))))
