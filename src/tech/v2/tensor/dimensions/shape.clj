(ns tech.v2.tensor.dimensions.shape
  "A shape vector entry can be a number of things.  We want to be precise
  with handling them and abstract that handling so new things have a clear
  path."
  (:require [tech.v2.tensor.utils
             :refer [when-not-error reversev map-reversev]]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.readers.range :as rdr-range]
            [tech.v2.datatype.readers.indexed :as indexed-rdr]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.monotonic-range :as dtype-range]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.protocols :as dtype-proto])
  (:import [tech.v2.datatype LongReader]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(def monotonic-operators
  {:+ <
   :- >})


(defn classify-sequence
  ^LongReader [item-seq]
  ;;Normalize this to account for single digit numbers
  (cond
    (number? item-seq)
    (assoc (dtype-range/scalar-range (long item-seq))
           :scalar? true)
    (dtype-proto/convertible-to-range? item-seq)
    (dtype-proto/->range item-seq :int64)
    :else
    (let [n-elems (dtype/ecount item-seq)
          ;;do it the hard way
          [min-item max-item] [(long (dfn/reduce-min item-seq))
                               (long (dfn/reduce-max item-seq))]
          min-item (long min-item)
          max-item (long max-item)]
      (if (= n-elems 1)
        (dtype-range/scalar-range (long min-item))
        (let [mon-op (->> monotonic-operators
                          (map (fn [[op-name op]]
                                 (when (apply op item-seq)
                                   op-name)))
                          (remove nil?)
                          first)]
          (if (and (= n-elems
                      (+ 1
                         (- max-item
                            min-item)))
                   mon-op)
            (if (= mon-op :+)
              (dtype-range/make-range min-item (inc max-item))
              (dtype-range/make-range max-item (dec min-item)))
            (dtype/->reader item-seq :int64)))))))


(defn classified-sequence?
  [item]
  (dtype-proto/convertible-to-range? item))


(defn classified-sequence->count
  ^long [item]
  (dtype-proto/ecount item))

(defn classified-sequence->reader
  ^LongReader [cseq]
  (dtype/->reader cseq :int64))


(defn classified-sequence-max
  ^long [item]
  (dtype-proto/range-max item))


(defn combine-classified-sequences
  "Room for optimization here.  But simplest way is easiest to get correct."
  [source-sequence select-sequence]
  (let [src-range? (dtype-proto/convertible-to-range? source-sequence)
        dst-range? (dtype-proto/convertible-to-range? select-sequence)]
    (if (and src-range? dst-range?)
      (-> (dtype-proto/combine-range
           (dtype-proto/->range source-sequence :int64)
           (dtype-proto/->range select-sequence :int64))
          (assoc :scalar? (:scalar? select-sequence)))
      (indexed-rdr/make-indexed-reader source-sequence select-sequence
                                       :datatype :int64))))


(defn classified-sequence->elem-idx
  ^long [item ^long shape-idx]
  (.read (typecast/datatype->reader :int64 item) shape-idx))


(defn classified-sequence->global-addr
  "Inverse of above"
  ^long [dim ^long shape-idx]
  (let [start (long (dtype-proto/range-start dim))
        increment (long (dtype-proto/range-increment dim))]
    (quot (- shape-idx start)
          increment)))


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
  (every? number? shape))


(defn indirect-shape?
  [shape]
  (not (direct-shape? shape)))


(defn ecount
  "Return the element count indicated by the dimension map"
  ^long [shape]
  (let [count-vec (shape->count-vec shape)
        n-items (count count-vec)]
    (loop [idx 0
           sum 0]
      (if (< idx n-items)
        (recur (unchecked-inc idx)
               (unchecked-add sum
                              (long (count-vec idx))))
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
