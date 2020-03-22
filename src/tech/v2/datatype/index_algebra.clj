(ns tech.v2.datatype.index-algebra
  "Operations on sets of indexes.  And index set is always representable by a long
  reader.  Indexes that are monotonically incrementing are special as they map to an
  underlying layer with the identity function enabling block transfers or operations
  against the data.  So it is imporant to classify distinct types of indexing
  operations."
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.v2.datatype.monotonic-range :as dtype-range]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.readers.indexed :as indexed-rdr]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.casting :as casting])
  (:import [tech.v2.datatype LongReader]
           [tech.v2.datatype.monotonic_range Int64Range]
           [clojure.lang IObj]
           [java.util Map]
           [clojure.lang MapEntry]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defprotocol PIndexAlgebra
  (offset [item offset])
  (broadcast [item num-repetitions])
  (offset? [item])
  (broadcast? [item])
  (simple? [item])
  (get-offset [item])
  (get-reader [item])
  (get-n-repetitions [item]))

(declare make-idx-alg)


(deftype IndexAlg [^LongReader reader ^long n-reader-elems
                   ^long offset ^long repetitions]
  LongReader
  (lsize [rdr] (* n-reader-elems repetitions))
  (read [rdr idx] (.read reader (rem (+ idx offset)
                                     n-reader-elems)))
  dtype-proto/PRangeConvertible
  (convertible-to-range? [item]
    (and (dtype-proto/convertible-to-range? reader)
         (simple? item)))
  (->range [item options] (dtype-proto/->range reader options))
  dtype-proto/PConstantTimeMinMax
  (has-constant-time-min-max? [item]
    (dtype-proto/has-constant-time-min-max? reader))
  (constant-time-min [item] (dtype-proto/constant-time-min reader))
  (constant-time-max [item] (dtype-proto/constant-time-max reader))
  PIndexAlgebra
  (offset [item new-offset]
    (IndexAlg. reader n-reader-elems
               (rem (+ offset (long new-offset))
                    n-reader-elems)
               repetitions))
  (broadcast [item num-repetitions]
    (IndexAlg. reader n-reader-elems
               offset (* repetitions (long num-repetitions))))
  (offset? [item] (not= 0 offset))
  (broadcast? [item] (not= 1 repetitions))
  (simple? [item] (and (== 0 offset)
                       (== 1 repetitions)))
  (get-offset [item] offset)
  (get-reader [item] reader)
  (get-n-repetitions [item] repetitions)
  dtype-proto/PBuffer
  (sub-buffer [item new-offset len]
    (let [new-offset (long new-offset)
          len (long len)]
      (when-not (<= (+ new-offset len) (.lsize item))
        (throw (Exception. "Sub buffer out of range.")))
      (when-not (> len 0)
        (throw (Exception. "Length is not > 0")))
      (let [rel-offset (rem (long (+ offset new-offset))
                            n-reader-elems)
            len (long len)]
        (if (<= (+ rel-offset len) n-reader-elems)
          (dtype-proto/sub-buffer reader rel-offset len)
          (reify LongReader
            (lsize [rdr] len)
            (read [rdr idx]
              (.read item (+ idx new-offset)))))))))


(defn maybe-range-reader
  "Create a range if possible.  If not, return a reader that knows the found mins
  and maxes."
  [^LongReader reader]
  (let [first-elem (.read reader 0)
        second-elem (.read reader 1)
        increment (- second-elem first-elem)
        n-elems (.lsize reader)]
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
        ;;Make a range if we can but if we cannot then at least maintain
        ;;constant min/max behavior
        (if constant-increment?
          (dtype-range/make-range (.read reader 0)
                                  (+ last-elem increment) increment)
          (reify
            LongReader
            (lsize [rdr] n-elems)
            (read [rdr idx] (.read reader idx))
            dtype-proto/PConstantTimeMinMax
            (has-constant-time-min-max? [item] true)
            (constant-time-min [item] item-min)
            (constant-time-max [item] item-max)))))))


(defn dimension->reader
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
          n-elems (dtype-base/ecount typed-buf)
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
    (instance? IndexAlg item-seq)
    item-seq
    :else
    (let [item-seq (if (dtype-proto/convertible-to-reader? item-seq)
                     item-seq
                     (long-array item-seq))
          n-elems (dtype-base/ecount item-seq)
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
        (maybe-range-reader reader)
        :else
        reader))))


(defn ->index-alg
  ^IndexAlg [data]
  (if (instance? IndexAlg data)
    data
    (let [rdr (dimension->reader data)]
      (IndexAlg. rdr (.lsize rdr) 0 1))))


(defn- simplify-reader
  [^LongReader reader]
  (let [n-elems (.lsize reader)]
    (cond
      (= n-elems 1)
      (dtype-range/make-range (.read reader 0) (inc (.read reader 0)))
      (= n-elems 2)
      (let [start (.read reader 0)
            last-elem (.read reader 1)
            increment (- last-elem start)]
        (dtype-range/make-range start (+ last-elem increment) increment))
      (<= n-elems 5)
      (maybe-range-reader reader)
      :else
      reader)))


(defn- simplify-range
  "Ranges starting at 0 and incrementing by 1 can be represented by numbers"
  [item]
  (if (dtype-proto/convertible-to-range? item)
    (let [item-rng (dtype-proto/->range item {})]
      (if (and (== 0 (long (dtype-proto/range-start item-rng)))
               (== 1 (long (dtype-proto/range-increment item-rng))))
        (dtype-proto/ecount item)
        item))
    item))


(defn select
  "Given a 'dimension', select and return a new 'dimension'.  A dimension could be
  a number which implies the range 0->number else it could be something convertible
  to a long reader.  This algorithm attempts to aggresively minimize the complexity
  of the returned dimension object.
  arguments may be
  :all - no change
  :lla - reverse index
  :number - select that index
  :sequence - select items in sequence.  Ranges will be better supported than truly
  random access.
  "
  [dim select-arg]
  (if (= select-arg :all)
    dim
    (let [rdr (dimension->reader dim)
          n-elems (.lsize rdr)]
      (if (number? select-arg)
        (let [select-arg (long select-arg)]
          (when-not (< select-arg (.lsize rdr))
            (throw (Exception. (format "Index out of range: %s >= %s"
                                       select-arg (.lsize rdr)))))
          (let [read-value (.read rdr select-arg)]
            (with-meta
              (dtype-range/make-range read-value (unchecked-inc read-value))
              {:select-scalar? true})))
        (simplify-range
         (let [^LongReader select-arg (if (= select-arg :lla)
                                        (dtype-range/reverse-range n-elems)
                                        (dimension->reader select-arg))
               n-select-arg (.lsize select-arg)]
           (if (dtype-proto/convertible-to-range? select-arg)
             (let [select-arg (dtype-proto/->range select-arg {})]
               (if (dtype-proto/convertible-to-range? dim)
                 (dtype-proto/combine-range (dtype-proto/->range dim {})
                                            select-arg)
                 (let [sel-arg-start (long (dtype-proto/range-start select-arg))
                       sel-arg-increment (long (dtype-proto/range-increment
                                                select-arg))
                       select-arg (dtype-proto/range-offset select-arg
                                                            (- sel-arg-start))
                       ;;the sub buffer operation here has a good chance of simplifying
                       ;;the dimension object.
                       rdr (dtype-proto/sub-buffer rdr
                                                   sel-arg-start
                                                   (* sel-arg-increment n-select-arg))]
                   (simplify-reader
                    (if (== 1 (long (dtype-proto/range-increment select-arg)))
                      rdr
                      (indexed-rdr/make-indexed-reader select-arg rdr))))))
             (simplify-reader
              (indexed-rdr/make-indexed-reader select-arg rdr)))))))))


(defn dense?
  "Are the indexes packed, increasing or decreasing by one."
  [dim]
  (boolean
   (or (number? dim)
       (and (dtype-proto/convertible-to-range? dim)
            (== 1 (long (dtype-proto/range-increment
                         (dtype-proto/->range dim {}))))))))

(defn direct?
  "Is the data represented natively, indexes starting at zero and incrementing by
  one?"
  [dim]
  (boolean
   (or (number? dim)
       (when-let [dim-range (when (dtype-proto/convertible-to-range? dim)
                              (dtype-proto/->range dim {}))]
         (and
          (== 1 (long (dtype-proto/range-increment dim-range)))
          (== 0 (long (dtype-proto/range-start dim-range))))))))



(extend-type Object
  PIndexAlgebra
  (offset [item offset-val]
    (offset (->index-alg item) offset-val))
  (broadcast [item num-repetitions]
    (broadcast (->index-alg item) num-repetitions))
  (offset? [item] false)
  (get-offset [item] 0)
  (broadcast? [item] false)
  (simple? [item] true)
  (get-offset [item] 0)
  (get-reader [item] item)
  (get-n-repetitions [item] 1))


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
    (let [group-map (dfn/arggroup-by identity
                                     (dtype-proto/->reader dim {:datatype
                                                                :int64}))]
      ;;Also painful!  But less so than repeated linear searches.
      (->> group-map
           (map (fn [[k v]]
                  [k (first v)]))
           (into {})))))
