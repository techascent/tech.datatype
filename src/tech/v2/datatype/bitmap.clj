(ns tech.v2.datatype.bitmap
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.typed-buffer :as typed-buffer]
            [tech.v2.datatype.clj-range :as clj-range]
            [tech.v2.datatype.array]
            [tech.v2.datatype.nio-buffer])
  (:import [it.unimi.dsi.fastutil.longs LongSet LongIterator]
           [org.roaringbitmap RoaringBitmap ImmutableBitmapDataProvider]
           [tech.v2.datatype SimpleLongSet LongReader LongBitmapIter]
           [clojure.lang IFn LongRange]
           [tech.v2.datatype.typed_buffer TypedBuffer]
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


(defn- construct-from-ints
  ^RoaringBitmap [item]
  (if-let [data (ensure-int-array item)]
    (RoaringBitmap/bitmapOf data)
    (throw (Exception. (format "Failed to construct roaring bitmap from item %s"
                               item)))))


(declare ->bitmap)


(extend-type RoaringBitmap
  dtype-proto/PDatatype
  (get-datatype [bitmap] :uint32)
  dtype-proto/PCountable
  (ecount [bitmap] (.getLongCardinality bitmap))
  dtype-proto/PToReader
  (convertible-to-reader? [bitmap] true)
  (->reader [bitmap options]
    (let [n-elems (dtype-base/ecount bitmap)]
      (-> (reify
            LongReader
            (getDatatype [rdr] :uint32)
            (lsize [rdr] n-elems)
            (read [rdr idx] (Integer/toUnsignedLong (.select bitmap (int idx))))
            dtype-proto/PConstantTimeMinMax
            (has-constant-time-min-max? [rdr] true)
            (constant-time-min [rdr] (.first bitmap))
            (constant-time-max [rdr] (.last bitmap))
            dtype-proto/PToBitmap
            (convertible-to-bitmap? [item] true)
            (as-roaring-bitmap [item] bitmap)
            Iterable
            (iterator [rdr] (LongBitmapIter. (.getIntIterator bitmap))))
          (dtype-proto/->reader options))))
  dtype-proto/PConstantTimeMinMax
  (has-constant-time-min-max? [bitmap] true)
  (constant-time-min [bitmap] (.first bitmap))
  (constant-time-max [bitmap] (.last bitmap))
  dtype-proto/PClone
  (clone [bitmap datatype]
    (when-not (= datatype (dtype-base/get-datatype bitmap))
      (throw (Exception. "Invalid datatype")))
    (.clone bitmap))
  dtype-proto/PToBitmap
  (convertible-to-bitmap? [item] true)
  (as-roaring-bitmap [item] item)
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


(defmethod print-method RoaringBitmap
  [buf w]
  (let [^java.io.Writer w w]
    (.write w "#")
    (.write w (.toString ^Object buf))))


(defn bitmap->typed-buffer
  [^RoaringBitmap bitmap]
  (TypedBuffer. :uint32 (.toArray bitmap)))


(deftype BitmapSet [^RoaringBitmap bitmap]
  SimpleLongSet
  (getDatatype [item] :uin32)
  (lsize [item] (.getLongCardinality bitmap))
  (lcontains [item arg] (.contains bitmap (unchecked-int arg)))
  (ladd [item arg] (.add bitmap (unchecked-int arg)) true)
  (lremove [item arg] (.remove bitmap (unchecked-int arg)) true)
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (dtype-proto/->reader bitmap options))
  dtype-proto/PClone
  (clone [item datatype]
    (BitmapSet. (dtype-proto/clone bitmap datatype)))
  dtype-proto/PConstantTimeMinMax
  (has-constant-time-min-max? [item] true)
  (constant-time-min [item] (.first bitmap))
  (constant-time-max [item] (.last bitmap))
  dtype-proto/PToBitmap
  (convertible-to-bitmap? [item] true)
  (as-roaring-bitmap [item] bitmap)
  Iterable
  (iterator [item] (.iterator ^Iterable (dtype-proto/->reader bitmap {})))
  dtype-proto/PBitmapSet
  (set-and [lhs rhs] (BitmapSet. (dtype-proto/set-and bitmap rhs)))
  (set-and-not [lhs rhs] (BitmapSet. (dtype-proto/set-and-not bitmap rhs)))
  (set-or [lhs rhs] (BitmapSet. (dtype-proto/set-or bitmap rhs)))
  (set-xor [lhs rhs] (BitmapSet. (dtype-proto/set-xor bitmap rhs)))
  (set-offset [item offset] (BitmapSet. (dtype-proto/set-offset bitmap offset)))
  (set-add-range! [item start end]
    (dtype-proto/set-add-range! bitmap start end)
    item)
  (set-add-block! [item data]
    (dtype-proto/set-add-block! item data)
    item)
  (set-remove-range! [item start end]
    (dtype-proto/set-remove-range! bitmap start end)
    item)
  (set-remove-block! [item data]
    (dtype-proto/set-remove-block! item data)
    item))


(defn ->bitmap
  (^RoaringBitmap [item]
   (cond
     (nil? item)
     (RoaringBitmap.)
     (dtype-proto/convertible-to-bitmap? item)
     (dtype-proto/as-roaring-bitmap item)
     (instance? LongRange item)
     (long-range->bitmap item)
     :else
     (if (= (dtype-base/get-datatype item) :uint32)
       (if-let [ary-buf (when-let [nio-buf (dtype-proto/->buffer-backing-store item)]
                          (dtype-proto/->sub-array nio-buf))]
         (if (instance? (Class/forName "[I") (:java-array ary-buf))
           (do
             (doto (RoaringBitmap.)
               (.addN ^ints (:java-array ary-buf)
                      (int (:offset ary-buf))
                      (int (:length ary-buf)))))
           (construct-from-ints item))
         (construct-from-ints item))
       (construct-from-ints item))))
  (^RoaringBitmap []
   (RoaringBitmap.)))


(defn ->unique-bitmap
  (^RoaringBitmap [item]
   (if (dtype-proto/convertible-to-bitmap? item)
     (.clone ^RoaringBitmap (dtype-proto/as-roaring-bitmap item))
     (->bitmap item)))
  (^RoaringBitmap []
   (RoaringBitmap.)))



(defn bitmap->efficient-random-access-reader
  [bitmap]
  (when (dtype-proto/convertible-to-bitmap? bitmap)
    (let [^RoaringBitmap bitmap (dtype-proto/as-roaring-bitmap bitmap)
          typed-buf (bitmap->typed-buffer bitmap)
          src-reader (typecast/datatype->reader :int64 typed-buf)
          n-elems (dtype-base/ecount typed-buf)
          cmin (dtype-proto/constant-time-min bitmap)
          cmax (dtype-proto/constant-time-max bitmap)]
      (reify
        LongReader
        (lsize [rdr] n-elems)
        (read [rdr idx] (.read src-reader idx))
        dtype-proto/PToBitmap
        (convertible-to-bitmap? [item] true)
        (as-roaring-bitmap [item] bitmap)
        dtype-proto/PConstantTimeMinMax
        (has-constant-time-min-max? [item] true)
        (constant-time-min [item] cmin)
        (constant-time-max [item] cmax)))))
