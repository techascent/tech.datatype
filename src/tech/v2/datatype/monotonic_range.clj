(ns tech.v2.datatype.monotonic-range
  "Ranges that *are* readers.  And that support some level of algebraic operations
  between pairs of them."
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.base :as base]
            ;;Complete clojure range support
            [tech.v2.datatype.clj-range :as clj-range])
  (:import [tech.v2.datatype LongReader]
           [clojure.lang LongRange]
           [java.lang.reflect Field]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare make-range)


(deftype Int64Range [^long start ^long increment ^long n-elems]
  dtype-proto/PDatatype
  (get-datatype [item] :int64)
  LongReader
  (lsize [item] n-elems)
  (read [item idx]
    (+ start (* increment idx)))
  dtype-proto/PBuffer
  (sub-buffer [item offset len]
    (let [offset (long offset)
          len (long len)]
      (when (> (- len offset) (.lsize item))
        (throw (Exception. "Length out of range")))
      (let [new-start (+ start (* offset increment))]
        (->Int64Range new-start
                      (+ new-start (* len increment))
                      increment))))
  dtype-proto/PRangeConvertible
  (convertible-to-range? [item] true)
  (->range [item datatype]
    (if (= datatype :int64)
      item
      (make-range start increment n-elems datatype)))

  dtype-proto/PRange
  (combine-range [lhs rhs]
    (let [r-start (long (dtype-proto/range-start rhs))
          r-n-elems (long (dtype-proto/ecount rhs))
          r-inc (long (dtype-proto/range-increment rhs))
          r-stop (+ r-start (* r-n-elems r-inc))
          new-start (+ start (* r-start increment))
          new-inc (* r-inc increment)]
      (when (or (> r-stop n-elems)
                (>= r-start n-elems))
        (throw (Exception. "Combined ranges - righthand side out of range")))
      (->Int64Range new-start new-inc r-n-elems)))
  (range-start [item] start)
  (range-increment [item] increment)
  (range-min [item] (if (> increment 0)
                      start
                      (+ start (* n-elems increment))))
  (range-max [item] (if (> increment 0)
                      (+ start (* n-elems increment))
                      start)))


(defn make-range
  ([start end increment datatype]
   (when-not (= datatype :int64)
     (throw (Exception. "Only long ranges supported for now")))
   (let [start (long start)
         end (long end)
         increment (long increment)]
     (when (== 0 increment)
       (throw (Exception. "Infinite range detected - zero increment")))
     (let [n-elems (if (> increment 0)
                     (quot (+ (max 0 (- end start))
                              (dec increment))
                           increment)
                     (quot (+ (min 0 (- end start))
                              (inc increment))
                           increment))]
       (->Int64Range start increment n-elems))))
  ([start end increment]
   (make-range start end increment (base/get-datatype start)))
  ([start end]
   (make-range start end 1))
  ([end]
   (make-range 0 end 1)))


(defn int64-scalar->range
  [scalar-value]
  (->Int64Range (long scalar-value) 1 1))


(extend-type LongRange
  dtype-proto/PRangeConvertible
  (convertible-to-range? [item] true)
  (->range [rng datatype]
    (when-not (= datatype :int64)
      (throw (Exception. "Only int64 ranges supported at this time.")))
    (let [start (long (first rng))
          step (long (.get ^Field clj-range/lr-step-field rng))
          n-elems (.count rng)]
      (->Int64Range start step n-elems))))


(defn reverse-range
  ([len]
   (make-range (unchecked-dec (long len)) -1 -1))
  ([start end]
   (make-range (unchecked-dec (long end)) start -1)))
