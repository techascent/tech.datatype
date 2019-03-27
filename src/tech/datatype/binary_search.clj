(ns tech.datatype.binary-search
  (:require [tech.datatype.typecast :as typecast]
            [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro datatype->binary-search-algo
  [datatype ary from to target]
  (case datatype
    :int8 `(ByteArrays/binarySearch ~ary ~from ~to ~target)
    :int16 `(ShortArrays/binarySearch ~ary ~from ~to ~target)
    :int32 `(IntArrays/binarySearch ~ary ~from ~to ~target)
    :int64 `(LongArrays/binarySearch ~ary ~from ~to ~target)
    :float32 `(FloatArrays/binarySearch ~ary ~from ~to ~target)
    :float64 `(DoubleArrays/binarySearch ~ary ~from ~to ~target)))


(defmacro make-binary-search
  [datatype]
  `(fn [values# target#]
     (let [target# (casting/datatype->cast-fn :unknown ~datatype target#)
           values# (typecast/datatype->reader ~datatype values# true)
           buf-ecount# (.size values#)]
       (if (= 0 buf-ecount#)
         [false 0]
         (loop [low# (int 0)
                high# (int buf-ecount#)]
           (if (< low# high#)
             (let [mid# (+ low# (quot (- high# low#) 2))
                   buf-data# (.read values# mid#)]
               (if (= buf-data# target#)
                 (recur mid# mid#)
                 (if (and (< buf-data# target#)
                          (not= mid# low#))
                   (recur mid# high#)
                   (recur low# mid#))))
             (let [buf-val# (.read values# low#)]
               (if (<= target# buf-val#)
                 [(= target# buf-val#) low#]
                 [false (unchecked-inc low#)]))))))))


(defmacro make-binary-search-table
  []
  `(->> [~@(for [dtype casting/host-numeric-types]
             [dtype `(make-binary-search ~dtype)])]
        (into {})))

(def binary-search-table (make-binary-search-table))


(defn binary-search
  "Perform binary search returning long idx of matching value or insert position.
  Returns index of the element or the index where it should be inserted.  Returns
  a tuple of [found? insert-or-elem-pos]"
  [values target {:keys [datatype]}]
  (let [datatype (or datatype (dtype-proto/get-datatype values))]
    (if-let [value-fn (get binary-search-table (casting/datatype->safe-host-type
                                                datatype))]
      (value-fn values target)
      [false 0])))
