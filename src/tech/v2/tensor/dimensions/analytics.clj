(ns tech.v2.tensor.dimensions.analytics
  (:require [tech.v2.tensor.dimensions.shape :as shape]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.index-algebra :as idx-alg]
            [tech.v2.datatype.functional :as dtype-fn]
            [tech.v2.datatype.protocols :as dtype-proto]
            [primitive-math :as pmath])
  (:import [tech.v2.datatype LongReader]
           [java.util List]
           [it.unimi.dsi.fastutil.longs LongList LongArrayList]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn buffer-ecount
  "What is the necessary ecount for a given buffer"
  ^long [{:keys [shape strides]}]
  ;;In this case the length of strides is so small as to make the general
  ;;method fast to just use object methods.
  (let [^List shape shape
        ^List strides strides
        stride-idx (int (dtype-fn/argmax {:datatype :int64} strides))
        stride-val (long (.get strides stride-idx))
        shape-val (.get shape stride-idx)
        shape-val (long
                   (cond
                     (number? shape-val)
                     shape-val
                     (dtype-proto/has-constant-time-min-max? shape-val)
                     (dtype-proto/constant-time-max shape-val)
                     :else
                     (apply max (dtype/->reader
                                 shape-val :int64))))]
    (* shape-val stride-val)))


(defn dims->shape-data
  [{:keys [shape strides shape-ecounts] :as dims}]
  (let [direct? (shape/direct-shape? shape)
        shape-ecount (long (apply * shape-ecounts))
        offsets? (boolean (some idx-alg/offset? shape))
        dense? (boolean (and direct?
                             (== shape-ecount
                                 (buffer-ecount dims))))
        increasing? (boolean (and direct?
                                  (apply >= strides)
                                  (not offsets?)))
        vec-shape (shape/shape->count-vec shape)
        broadcast? (boolean (some idx-alg/broadcast? shape))]
    {:direct? direct?
     :dense? dense?
     :increasing? increasing?
     :broadcast? broadcast?
     :offsets? offsets?
     :n-dims (count shape)
     :shape-ecount shape-ecount
     :vec-shape vec-shape}))


(defn shape-data->signature
  [shape-data]
  (dissoc shape-data :shape-ecount :vec-shape))


(defn shape-ary->strides
  "Strides assuming everything is increasing and packed"
  ^LongList [^List shape-vec]
  (let [retval (long-array (count shape-vec))
        n-elems (alength retval)
        n-elems-dec (dec n-elems)]
    (loop [idx n-elems-dec
           last-stride 1]
      (if (>= idx 0)
        (let [next-stride (* last-stride
                             (if (== idx n-elems-dec)
                               1
                               (long (.get shape-vec (inc idx)))))]
          (aset retval idx next-stride)
          (recur (dec idx) next-stride))
        (LongArrayList/wrap retval)))))


(defn find-breaks
  "Attempt to reduce the dimensionality of the shape but do not
  change its elementwise global->local definition.  This is done because
  the running time of elementwise global->local is heavily dependent upon the
  number of dimensions in the shape."
  ([^List shape ^List strides ^List shape-ecounts]
   (let [n-elems (count shape)
         n-elems-dec (dec n-elems)]
     (loop [idx n-elems-dec
            last-stride 1
            last-break n-elems
            breaks nil]
       (if (>= idx 0)
         (let [last-shape-entry (if (== idx n-elems-dec)
                                  nil
                                  (.get shape (inc idx)))
               last-entry-number? (number? last-shape-entry)
               next-stride (pmath/* last-stride
                                    (if (== idx n-elems-dec)
                                      1
                                      (if last-entry-number?
                                        (long last-shape-entry)
                                        -1)))
               shape-entry (.get shape idx)
               shape-entry-number? (number? shape-entry)
               current-offset? (idx-alg/offset? shape-entry)
               last-offset? (if last-shape-entry
                              (idx-alg/offset? last-shape-entry)
                              current-offset?)
               stride-val (long (.get strides idx))
               next-break
               (if (and (pmath/== stride-val
                                  next-stride)
                        shape-entry-number?
                        (and last-entry-number?
                             (pmath/== (long last-shape-entry)
                                       (long (.get shape-ecounts (inc idx)))))
                        (not current-offset?)
                        (not last-offset?))
                 last-break
                 (inc idx))
               breaks (if (== next-break last-break)
                        breaks
                        (conj breaks (range next-break last-break)))]
           (recur (dec idx) stride-val next-break breaks))
         (conj breaks (range 0 last-break))))))
  ([{:keys [shape strides shape-ecounts]}]
   (find-breaks shape strides shape-ecounts)))


(defn reduce-dimensionality
  "Make a smaller equivalent shape in terms of row-major addressing
  from the given shape."
  ([{:keys [shape strides shape-ecounts]}
    offsets?]
   (let [^List shape shape
         ^List strides strides
         ^List shape-ecounts shape-ecounts
         breaks (find-breaks shape strides shape-ecounts)
         shape-ecounts (mapv
                        #(reduce * (map (fn [idx] (.get shape-ecounts (long idx)))
                                        %))
                        breaks)]
     {:shape (mapv #(if (== 1 (count %))
                      (.get shape (long (first %)))
                     (apply * (map (fn [idx]
                                     (.get shape (long idx)))
                                   %)))
                  breaks)
      :strides (mapv
                #(reduce min (map (fn [idx]
                                    (.get strides (long idx)))
                                  %))
                breaks)
      :shape-ecounts shape-ecounts
      :shape-ecount-strides (shape-ary->strides shape-ecounts)}))
  ([dims]
   (reduce-dimensionality dims (any-offsets? dims))))


(defn dims->reduced-dims
  [dims]
  (or (:reduced-dims dims) (reduce-dimensionality dims)))


(defn are-dims-bcast?
  "Fast version of broadcasting check for when we know the types
  the types of shapes and max-shape"
  [reduced-dims]
  (boolean (some idx-alg/broadcast? (:shape reduced-dims))))


(comment
  (reduce-dimensionality {:shape [4 4 4]
                          :strides [16 4 1]
                          :shape-ecounts [4 4 4]})
  )
