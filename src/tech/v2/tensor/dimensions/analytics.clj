(ns tech.v2.tensor.dimensions.analytics
  (:require [tech.v2.tensor.dimensions.shape :as shape]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dtype-fn]
            [primitive-math :as pmath])
  (:import [tech.v2.datatype LongReader]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn buffer-ecount
  "What is the necessary ecount for a given buffer"
  ^long [{:keys [shape strides]}]
  ;;In this case the length of strides is so small as to make the general
  ;;method fast to just use object methods.
  (let [stride-idx (dtype-fn/argmax {:datatype :object} strides)
        stride-val (dtype/get-value strides stride-idx)
        shape-val (dtype/get-value shape stride-idx)
        shape-val (long
                   (cond
                     (number? shape-val)
                     shape-val
                     (shape/classified-sequence? shape-val)
                     (shape/classified-sequence-max shape-val)
                     :else
                     (apply max (dtype/->reader
                                 shape-val :int32))))]
    (* shape-val (long stride-val))))


(defn any-offsets?
  [{:keys [offsets]}]
  (not (every? #(== 0 (long %)) offsets)))


(defn dims->shape-data
  [{:keys [shape strides max-shape] :as dims}]
  (let [direct? (shape/direct-shape? shape)
        shape-ecount (long (apply * max-shape))
        offsets? (any-offsets? dims)
        dense? (if direct?
                 (= shape-ecount
                    (buffer-ecount dims))
                 false)
        increasing? (if direct?
                      (and (apply >= strides)
                           (not offsets?))
                      false)
        vec-shape (shape/shape->count-vec shape)
        broadcast? (not= vec-shape max-shape)]
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
  ^longs [^longs shape-vec]
  (let [retval (long-array (count shape-vec))
        n-elems (alength retval)
        n-elems-dec (dec n-elems)]
    (loop [idx n-elems-dec
           last-stride 1]
      (if (>= idx 0)
        (let [next-stride (* last-stride
                             (if (== idx n-elems-dec)
                               1
                               (aget shape-vec (inc idx))))]
          (aset retval idx next-stride)
          (recur (dec idx) next-stride))
        retval))))


(defn find-breaks
  "Attempt to reduce the dimensionality of the shape but do not
  change its elementwise global->local definition.  This is done because
  the running time of elementwise global->local is heavily dependent upon the
  number of dimensions in the shape."
  ([^objects shape ^longs strides
    ^longs max-shape ^longs offsets]
   (let [n-elems (alength shape)
         n-elems-dec (dec n-elems)]
     (loop [idx n-elems-dec
            last-stride 1
            last-break n-elems
            breaks nil]
       (if (>= idx 0)
         (let [last-shape-entry (if (== idx n-elems-dec)
                                  nil
                                  (aget shape (inc idx)))
               last-entry-number? (number? last-shape-entry)
               next-stride (pmath/* last-stride
                                    (if (== idx n-elems-dec)
                                      1
                                      (if last-entry-number?
                                        (long last-shape-entry)
                                        -1)))
               current-offset (aget offsets idx)
               last-offset (if (== idx n-elems-dec)
                             current-offset
                             (aget offsets (inc idx)))
               shape-entry (aget shape idx)
               shape-entry-number? (number? shape-entry)
               next-break
               (if (and (pmath/== (aget strides idx)
                                  next-stride)
                        shape-entry-number?
                        (and last-entry-number?
                             (pmath/== (long last-shape-entry)
                                       (aget max-shape (inc idx))))
                        (== current-offset last-offset)
                        (== last-offset 0))
                 last-break
                 (inc idx))
               breaks (if (== next-break last-break)
                        breaks
                        (conj breaks (range next-break last-break)))]
           (recur (dec idx) (aget strides idx) next-break breaks))
         (conj breaks (range 0 last-break))))))
  ([{:keys [shape strides offsets max-shape] :as dims}]
   (find-breaks (object-array shape)
                (long-array strides)
                (long-array max-shape)
                (long-array offsets))))


(defn- shape-entry->long-or-reader
  "Shapes contain either long objects or readers at this point."
  [shape-entry]
  (cond
    (number? shape-entry)
    (long shape-entry)
    (shape/classified-sequence? shape-entry)
    (shape/classified-sequence->reader shape-entry)
    :else
    (dtype/->reader shape-entry :int64)))


(defn reduce-dimensionality
  "Make a smaller equivalent shape in terms of row-major addressing
  from the given shape."
  ([{:keys [shape strides offsets max-shape] :as dims}
    offsets?]
   (let [shape (object-array shape)
         strides (long-array strides)
         offsets (long-array offsets)
         max-shape (long-array max-shape)
         breaks (find-breaks shape strides max-shape offsets)
         max-shape (long-array (map
                                #(reduce * (map (fn [idx] (aget max-shape (long idx)))
                                                %))
                                breaks))]
     {:shape (object-array (map #(if (== 1 (count %))
                                   (->
                                    (aget shape (long (first %)))
                                    (shape-entry->long-or-reader))
                                   (apply * (map (fn [idx]
                                                   (aget shape (long idx)))
                                                 %)))
                                breaks))
      :strides (long-array (map
                            #(reduce min (map (fn [idx] (aget strides (long idx)))
                                            %))
                            breaks))
      :offsets (when offsets?
                 (long-array (map
                              #(reduce + (map (fn [idx] (aget offsets (long idx)))
                                              %))
                              breaks)))
      :max-shape max-shape
      :max-shape-strides (shape-ary->strides max-shape)}))
  ([{:keys [shape strides offsets max-shape] :as dims}]
   (reduce-dimensionality dims (any-offsets? dims))))


(defn are-dims-bcast?
  "Fast version of broadcasting check for when we know the types
  the types of shapes and max-shape"
  [reduced-dims]
  (let [^objects shape (:shape reduced-dims)
        ^longs max-shape (:max-shape reduced-dims)
        n-elems (alength shape)]
    (loop [idx 0
           bcast? false]
      (if (and (< idx n-elems)
               (not bcast?))
        (let [shape-entry (aget shape idx)
              shape-ecount (if (number? shape-entry)
                             (long shape-entry)
                             (.lsize ^LongReader shape-entry))]
          (recur (pmath/inc idx)
                 (not (pmath/== shape-ecount (aget max-shape idx)))))
        bcast?))))
