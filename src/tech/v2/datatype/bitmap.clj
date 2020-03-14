(ns tech.v2.datatype.bitmap
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.clj-range :as clj-range])
  (:import [it.unimi.dsi.fastutil.longs LongSet LongIterator]
           [org.roaringbitmap RoaringBitmap]
           [tech.v2.datatype SimpleLongSet]
           [clojure.lang IFn LongRange]
           [java.lang.reflect Field]))

(set! *warn-on-reflection* true)


(deftype BitmapUIntSet [^RoaringBitmap bitmap]
  SimpleLongSet
  (getDatatype [this] :uint32)
  (lsize [this] (.getLongCardinality bitmap))
  (ladd [this lval]
    (.add bitmap
          (casting/datatype->unchecked-cast-fn
           :int64
           :uint32 lval))
    false)
  (lcontains [this lval]
    (.contains bitmap
               (casting/datatype->unchecked-cast-fn
                :int64
                :uint32 lval)))
  (lremove [this lval]
    (.remove bitmap
             (casting/datatype->unchecked-cast-fn
              :int64
              :uint32 lval))
    false)
  dtype-proto/PToArray
  (->sub-array [this] nil)
  (->array-copy [this] (.toArray bitmap))
  dtype-proto/PClone
  (clone [this datatype]
    (BitmapUIntSet. (.clone bitmap)))
  IFn
  (invoke [this item]
    (when (.contains this (long item))
      item
      nil))
  (applyTo [this argseq]
    (when-not (= 1 (count argseq))
      (throw (Exception. (format "Invoke only overloaded for 1 arg, %d provided."
                                 (count argseq)))))
    (.invoke this (first argseq)))
  Iterable
  (iterator [this]
    (let [int-iter (.getIntIterator bitmap)]
      (reify LongIterator
        (hasNext [this] (.hasNext int-iter))
        (nextLong [this]
          (Integer/toUnsignedLong
           (.next int-iter)))))))


(defn- long-range->bitmap
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


(def int-array-class (Class/forName "[I"))


(defn- ensure-int-array
  ^ints [item]
  (when-not (instance? int-array-class item)
    (-> (dtype-proto/make-container
         :typed-buffer
         :uint32
         item {})
        (dtype-proto/->buffer-backing-store)
        (dtype-proto/->sub-array)
        :java-array)))



(defn ->roaring-bitmap
  ^RoaringBitmap [item]
  (cond
    (instance? RoaringBitmap item)
    item
    (instance? BitmapUIntSet item)
    (.bitmap ^BitmapUIntSet item)
    (instance? LongRange item)
    (long-range->bitmap item)
    (and item (or (dtype-proto/convertible-to-reader? item)
                  (instance? java.util.Set item)))
    (doto (RoaringBitmap.)
      (.add ^ints (ensure-int-array item)))
    :else
    (throw (Exception. "Expected bitmap uint set or something convertible to"))))


(extend-type BitmapUIntSet
  dtype-proto/PSetOps
  (set-and [lhs rhs] (BitmapUIntSet.
                      (RoaringBitmap/and (.bitmap lhs)
                                         (->roaring-bitmap rhs))))
  (set-and-not [lhs rhs]
    (BitmapUIntSet.
     (RoaringBitmap/andNot (.bitmap lhs)
                           (->roaring-bitmap rhs))))
  (set-or [lhs rhs]
    (BitmapUIntSet.
     (RoaringBitmap/or (.bitmap lhs)
                           (->roaring-bitmap rhs))))
  (set-xor [lhs rhs]
    (BitmapUIntSet.
     (RoaringBitmap/xor (.bitmap lhs)
                        (->roaring-bitmap rhs))))
  (offset-set [item offset]
    (BitmapUIntSet.
     (RoaringBitmap/addOffset ^RoaringBitmap (.bitmap item) (unchecked-int offset))))
  (set-add-range! [item start end]
    (.add ^RoaringBitmap (.bitmap item)
          (unchecked-int start) (unchecked-int end))
    item)
  (set-add-block! [item data]
    (.add ^RoaringBitmap (.bitmap item) ^ints (ensure-int-array data))
    item)
  (set-remove-range! [item start end]
    (.remove ^RoaringBitmap (.bitmap item)
             (unchecked-int start) (unchecked-int end))
    item)
  (set-remove-block! [item data]
    (.remove ^RoaringBitmap (.bitmap item) ^ints (ensure-int-array data))
    item))


(defn make-bitmap-set
  ([]
   (BitmapUIntSet. (RoaringBitmap.)))
  ([elem-seq]
   (if (or (instance? BitmapUIntSet elem-seq)
           (instance? RoaringBitmap elem-seq))
     ;;Do not allow the sets to share backing stores.
     (BitmapUIntSet. (.clone (->roaring-bitmap elem-seq)))
     (BitmapUIntSet. (->roaring-bitmap elem-seq)))))
