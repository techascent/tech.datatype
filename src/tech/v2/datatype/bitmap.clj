(ns tech.v2.datatype.bitmap
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.clj-range :as clj-range])
  (:import [it.unimi.dsi.fastutil.longs LongSet LongIterator]
           [org.roaringbitmap RoaringBitmap]
           [tech.v2.datatype SimpleLongSet LongReader LongBitmapIter]
           [clojure.lang IFn LongRange]
           [java.lang.reflect Field]))


(set! *warn-on-reflection* true)


(def int-array-class (Class/forName "[I"))


(defn ensure-int-array
  ^ints [item]
  (when-not (instance? int-array-class item)
    (-> (dtype-proto/make-container
         :typed-buffer
         :uint32
         item {})
        (dtype-proto/->buffer-backing-store)
        (dtype-proto/->sub-array)
        :java-array)))


(defn long-range->bitmap
  [^LongRange item]
  (let [long-reader (typecast/datatype->reader :int64 item)
        step (long (.get ^Field clj-range/lr-step-field item))
        n-elems (.lsize long-reader)]
    (when-not (== 1 step)
      (throw (Exception.
              "Only monotonically incrementing ranges can be made into bitmaps")))
    (if (= 0 n-elems)
      (RoaringBitmap.)
      (let [start (.read long-reader 0)]
        (doto (RoaringBitmap.)
          (.add (unchecked-int start)
                (unchecked-int (+ start n-elems))))))))


(defn ->bitmap
  ^RoaringBitmap [item]
  (cond
    (instance? RoaringBitmap item)
    item
    (instance? LongRange item)
    (long-range->bitmap item)
    :else
    (doto (RoaringBitmap.)
      (.add ^ints (ensure-int-array item)))))


(extend-type RoaringBitmap
  dtype-proto/PDatatype
  (get-datatype [bitmap] :uint32)
  dtype-proto/PCountable
  (ecount [bitmap] (.getLongCardinality bitmap))
  dtype-proto/PToArray
  (->sub-array [bitmap] nil)
  (->array-copy [bitmap] (.toArray bitmap))
  dtype-proto/PToReader
  (convertible-to-reader? [bitmap] true)
  (->reader [bitmap options]
    (let [n-elems (dtype-base/ecount bitmap)]
      (-> (reify
            LongReader
            (getDatatype [rdr] :uint32)
            (lsize [rdr] n-elems)
            (read [rdr idx] (Integer/toUnsignedLong (.select bitmap (int idx))))
            Iterable
            (iterator [rdr] (LongBitmapIter. (.getIntIterator bitmap))))
          (dtype-proto/->reader options))))
  dtype-proto/PBitmapSet
  (set-and [lhs rhs] (RoaringBitmap/and lhs (->bitmap rhs)))
  (set-and-not [lhs rhs] (RoaringBitmap/andNot lhs (->bitmap rhs)))
  (set-or [lhs rhs] (RoaringBitmap/or lhs (->bitmap rhs)))
  (set-xor [lhs rhs] (RoaringBitmap/xor lhs (->bitmap rhs)))
  (set-offset [bitmap offset] (RoaringBitmap/addOffset bitmap (unchecked-int offset)))
  (set-add-range! [bitmap start end]
    (.add bitmap (unchecked-int start) (unchecked-int end))
    bitmap)
  (set-add-block! [bitmap data]
    (.add bitmap ^ints (ensure-int-array data))
    bitmap)
  (set-remove-range! [bitmap start end]
    (.remove bitmap (unchecked-int start) (unchecked-int end))
    bitmap)
  (set-remove-block! [bitmap data]
    (.remove bitmap ^ints (ensure-int-array data))
    bitmap))
