(ns tech.v2.tensor.dimensions.shape
  "A shape vector entry can be a number of things.  We want to be precise
  with handling them and abstract that handling so new things have a clear
  path."
  (:require [tech.v2.tensor.utils
             :refer [when-not-error reversev map-reversev]]
            [tech.v2.datatype :as dtype]
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


(defn classify-sequence
  "Given a generic thing, make the appropriate longreader that is geared towards rapid random access
  and, when possible, has constant time min/max operations."
  ^LongReader [item-seq]
  ;;Normalize this to account for single digit numbers
  (cond
    (number? item-seq)
    (with-meta
      (dtype-range/make-range (long item-seq))
      {:scalar? true})
    (dtype-proto/convertible-to-range? item-seq)
    (dtype-proto/->range item-seq {})
    (dtype-proto/convertible-to-bitmap? item-seq)
    (let [bitmap (dtype-proto/as-roaring-bitmap item-seq)
          ;;Random access on bitmaps is very bad.  So we create a typed buffer.
          typed-buf (bitmap/bitmap->typed-buffer bitmap)
          src-reader (typecast/datatype->reader :int64 typed-buf)
          n-elems (dtype/ecount typed-buf)
          cmin (dtype-proto/constant-time-min bitmap)
          cmax (dtype-proto/constant-time-max bitmap)]
      (reify
        LongReader
        (lsize [rdr] n-elems)
        (read [rdr idx] (.read src-reader idx))
        dtype-proto/PConstantTimeMinMax
        (has-constant-time-min-max? [item] true)
        (constant-time-min [item] cmin)
        (constant-time-max [item] cmax)))
    :else
    (let [item-seq (if (dtype/reader? item-seq)
                     item-seq
                     (long-array item-seq))
          n-elems (dtype/ecount item-seq)
          reader (typecast/datatype->reader :int64 item-seq)]
      (cond
        (= n-elems 1) (dtype-range/make-range (.read reader 0) (inc (.read reader 0)))
        (= n-elems 2)
        (let [start (.read reader 0)
              last-elem (.read reader 1)
              increment (- last-elem start)]
          (dtype-range/make-range start (+ last-elem increment) increment))
        ;;Try to catch quick,hand made ranges out of persistent vectors and such.
        (<= n-elems 5)
        (let [first-elem (.read reader 0)
              second-elem (.read reader 1)
              increment (- second-elem first-elem)]
          (loop [item-min (min (.read reader 0)
                               (.read reader 1))
                 item-max (max (.read reader 0)
                               (.read reader 1))
                 last-elem (.read reader 1)
                 constant-increment? true
                 idx 2]
            (if (< idx n-elems)
              (let [next-elem (.read reader idx)
                    next-increment (- next-elem last-elem)
                    item-min (min item-min next-elem)
                    item-max (max item-max next-elem)]
                (recur item-min item-max next-elem
                       (boolean (and constant-increment?
                                     (= next-increment increment)))
                       (unchecked-inc idx)))
              ;;Make a range if we can but if we cannot then at least maintain constant min/max behavior
              (if constant-increment?
                (dtype-range/make-range (.read reader 0) (+ last-elem increment) increment)
                (reify
                  LongReader
                  (lsize [rdr] n-elems)
                  (read [rdr idx] (.read reader idx))
                  dtype-proto/PConstantTimeMinMax
                  (has-constant-time-min-max? [item] true)
                  (constant-time-min [item] item-min)
                  (constant-time-max [item] item-max))))))
        :else
        reader))))


(defn classified-sequence->count
  ^long [^LongReader item]
  (.lsize item))


(defn classified-sequence->reader
  ^LongReader [cseq]
  cseq)


(defn combine-classified-sequences
  "Room for optimization here.  But simplest way is easiest to get correct."
  ^LongReader [source-sequence select-sequence]
  (let [src-range? (dtype-proto/convertible-to-range? source-sequence)
        dst-range? (dtype-proto/convertible-to-range? select-sequence)]
    (if (and src-range? dst-range?)
      (-> (dtype-proto/combine-range
           (dtype-proto/->range source-sequence :int64)
           (dtype-proto/->range select-sequence :int64))
          (with-meta (meta select-sequence)))
      ;;Hit a simplification here if we can.
      (classify-sequence
       (indexed-rdr/make-indexed-reader source-sequence select-sequence
                                        :datatype :int64)))))


(defn classified-sequence->elem-idx
  ^long [^LongReader item ^long shape-idx]
  (.read item shape-idx))


(defn dimension->reverse-long-map
  "This could be expensive in a lot of situations.  Hopefully the sequence is a range.
  We return a map that does the sparse reverse mapping on get.  IF there are multiple
  right answers we return the first one."
  ^Map [dim]
  (cond
    (number? dim)
    (let [dim (long dim)]
      (reify Map
        (size [m] (unchecked-int dim))
        (containsKey [m arg]
          (and arg
               (casting/integer-type?
                (dtype-proto/get-datatype arg))
               (let [arg (long arg)]
                 (and (>= arg 0)
                      (< arg dim)))))
        (isEmpty [m] (== dim 0))
        (entrySet [m]
          (->> (range dim)
               (map-indexed (fn [idx range-val]
                              (MapEntry. range-val idx)))
               set))
        (getOrDefault [m k default-value]
          (if (and k (casting/integer-type? (dtype-proto/get-datatype k)))
            (let [arg (long k)]
              (if (and (>= arg 0)
                       (< arg dim))
                arg
                default-value))
            default-value))
        (get [m k] (long k))))
    (dtype-proto/convertible-to-range? dim)
    (dtype-proto/range->reverse-map (dtype-proto/->range dim {}))
    :else
    (let [group-map (dfn/arggroup-by identity (dtype/->reader dim :int64))]
      ;;Also painful!  But less so than repeated linear searches.
      (->> group-map
           (map (fn [[k v]]
                  [k (first v)]))
           (into {})))))


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
