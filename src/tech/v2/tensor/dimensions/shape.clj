(ns tech.v2.tensor.dimensions.shape
  "A shape vector entry can be a number of things.  We want to be precise
  with handling them and abstract that handling so new things have a clear
  path."
  (:require [tech.v2.tensor.utils
             :refer [when-not-error reversev map-reversev]]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.index-algebra :as idx-alg]
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.v2.datatype.readers.range :as rdr-range]
            [tech.v2.datatype.readers.indexed :as indexed-rdr]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.monotonic-range :as dtype-range]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting])
  (:import [tech.v2.datatype LongReader]
           [java.util Map]
           [clojure.lang MapEntry]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn shape-entry->count
  "Return a vector of counts of each shape."
  ^long [shape-entry]
  (if (number? shape-entry)
    (long shape-entry)
    (dtype/ecount shape-entry)))


(defn shape->count-vec
  ^longs [shape-vec]
  (mapv shape-entry->count shape-vec))


(defn direct-shape?
  [shape]
  (every? idx-alg/direct? shape))


(defn indirect-shape?
  [shape]
  (boolean (some (complement idx-alg/direct?) shape)))


(defn ecount
  "Return the element count indicated by the dimension map"
  ^long [shape]
  (let [count-vec (shape->count-vec shape)
        n-items (count count-vec)]
    (loop [idx 0
           sum 1]
      (if (< idx n-items)
        (recur (unchecked-inc idx)
               (* (long (count-vec idx))
                  sum))
        sum))))


(defn- ensure-direct
  [shape-seq]
  (when-not (direct-shape? shape-seq)
    (throw (ex-info "Index buffers not supported for this operation." {})))
  shape-seq)


(defn ->2d
  "Given dimensions, return new dimensions with the lowest (fastest-changing) dimension
  unchanged and the rest of the dimensions multiplied into the higher dimension."
  [shape]
  (when-not-error (seq shape)
    "Invalid shape in dimension map"
    {:shape shape})
  (if (= 1 (count shape))
    [1 (first shape)]
    [(apply * (ensure-direct (drop-last shape))) (last shape)]))


(defn ->batch
  "Given dimensions, return new dimensions with the lowest (fastest-changing) dimension
  unchanged and the rest of the dimensions multiplied into the higher dimension."
  [shape]
  (when-not-error (seq shape)
    "Invalid shape in dimension map"
    {:shape shape})
  (if (= 1 (count shape))
    [1 (first shape)]
    [(first shape) (apply * (ensure-direct (drop 1 shape)))]))
