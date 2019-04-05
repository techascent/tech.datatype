(ns tech.tensor.dimensions
  "Compute tensors dimensions control the shape and stride of the tensor along with
  offsetting into the actual data buffer.  This allows multiple backends to share a
  single implementation of a system that will allow transpose, reshape, etc. assuming
  the backend correctly interprets the shape and stride of the dimension objects.

  Shape vectors may have an index buffer in them at a specific dimension instead of a
  number.  This means that that dimension should be indexed indirectly.  If a shape has
  any index buffers then it is considered an indirect shape."
  (:require [clojure.core.matrix :as m]
            [tech.datatype :as dtype]
            [tech.tensor.dimensions.select :as dims-select]
            [tech.tensor.dimensions.shape :as shape]
            [tech.tensor.utils
             :refer [when-not-error reversev map-reversev]]))


(defn extend-strides
  [shape strides]
  (let [rev-strides (reversev strides)
        rev-shape (shape/reverse-shape shape)]
   (->> (reduce (fn [new-strides dim-idx]
                  (let [dim-idx (long dim-idx)
                        cur-stride (get rev-strides dim-idx)]
                    (if (= 0 dim-idx)
                      (conj new-strides (or cur-stride 1))
                      (let [last-idx (dec dim-idx)
                            last-stride (long (get new-strides last-idx))
                            cur-dim (long (get rev-shape last-idx))
                            min-next-stride (* last-stride cur-dim)]
                        (conj new-strides (or cur-stride min-next-stride))))))
                []
                (range (count shape)))
        reverse
        vec)))


(defn dimensions
  "A dimension is a map with at least a shape (vector of integers or index buffers) and
  potentially another vector of dimension names.  By convention the first member of the
  shape is the slowest changing and the last member of the shape is the most rapidly
  changing.  There can also be optionally a companion vector of names which name each
  dimension.  Names are used when doing things that are dimension aware such as a 2d
  convolution.  Shape is the same as a core-matrix shape."
  [shape & {:keys [names strides]}]
  (let [strides (extend-strides shape strides)
        sorted-shape-stride (->> (map vector shape strides)
                                 (sort-by second >))
        max-stride (apply max 0 (map second sorted-shape-stride))
        elem-count (apply * 1 (drop 1 (map (comp shape/shape-entry->count first)
                                           sorted-shape-stride)))]
    (when-not-error (<= (long elem-count)
                        (long max-stride))
      "Stride appears to be too small for element count"
      {:max-stride max-stride
       :elem-count elem-count
       :strides strides
       :shape shape})
    {:shape (vec shape)
     :strides strides
     :names names}))


(defn ecount
  "Return the element count indicated by the dimension map"
  ^long [{:keys [shape]}]
  (long (apply * (shape/shape->count-vec shape))))


(defn ->2d-shape
  "Given dimensions, return new dimensions with the lowest (fastest-changing) dimension
  unchanged and the rest of the dimensions multiplied into the higher dimension."
  [{:keys [shape]}]
  (shape/->2d shape))


(defn ->batch-shape
  "Given dimensions, return new dimensions with the lowest (fastest-changing) dimension
  unchanged and the rest of the dimensions multiplied into the higher dimension."
  [{:keys [shape]}]
  (shape/->2d shape))


(defn shape
  [{:keys [shape]}]
  (shape/shape->count-vec shape))


(defn strides
  ^long [{:keys [strides]}]
  strides)


(defn dense?
  [{:keys [shape strides]}]
  (and (shape/direct-shape? shape)
       (if (= 1 (count shape))
         (= 1 (long (first strides)))
         (let [[shape strides] (->> (map vector shape strides)
                                    ;;remove trivial shape/stride combinations
                                    (remove #(= 1 (first %)))
                                    ;;sort into known orientation
                                    (sort-by second >)
                                    ;;replace back to original
                                    ((fn [shp-strd]
                                       [(mapv first shp-strd)
                                        (mapv second shp-strd)])))
               ;;Given there was a remove we could have nothing left.
               max-stride (first strides)
               shape-num (apply * 1 (drop 1 shape))]
           (= max-stride shape-num)))))


(defn direct?
  [{:keys [shape]}]
  (shape/direct-shape? shape))


(defn indirect?
  [dims]
  (not (direct? dims)))


(defn access-increasing?
  "Are these dimensions setup such a naive seq through the data will be accessing memory
  in order.  This is necessary for external library interfaces (blas, cudnn).  An
  example would be after any nontrivial transpose that is not made concrete (copied)
  this condition will not hold."
  [{:keys [shape strides] :as dims}]
  (and (shape/direct-shape? shape)
       (apply >= strides)))


(defn ->most-rapidly-changing-dimension
  "Get the size of the most rapidly changing dimension"
  ^long [{:keys [shape]}]
  (shape/shape-entry->count (last shape)))


(defn ->least-rapidly-changing-dimension
  "Get the size of the least rapidly changing dimension"
  ^long [{:keys [shape]}]
  (shape/shape-entry->count (first shape)))


(defn elem-idx->addr
  "Given an arbitary logical element index, return the address of the element as
  calculated by waking through the shape from most rapidly changing to least rapidly
  changing and summing the shape index by the stride.

  Precondition:  rev-shape, rev-max-shape, strides are same length.
  rev-max-shape: maxes of all shapes passed in, reversed
  rev-shape: reverse shape.
  rev-strides: reverse strides.
  arg: >= 0."
  ^long [rev-shape rev-strides rev-max-shape arg]
  (long (let [num-items (count rev-shape)]
          (loop [idx (long 0)
                 arg (long arg)
                 offset (long 0)]
            (if (< idx num-items)
              (let [next-max (long (rev-max-shape idx))
                    next-stride (long (rev-strides idx))
                    next-dim-entry (rev-shape idx)
                    next-dim (shape/shape-entry->count next-dim-entry)
                    max-idx (rem arg next-max)
                    shape-idx (rem arg next-dim)]
                (recur (inc idx)
                       (quot arg next-max)
                       (+ offset (* next-stride
                                    (cond
                                      (number? next-dim-entry)
                                      shape-idx
                                      (shape/classified-sequence?
                                       next-dim-entry)
                                      (shape/classified-sequence->elem-idx
                                       next-dim-entry
                                       shape-idx)
                                      :else
                                      (long (dtype/get-value
                                             next-dim-entry
                                             shape-idx)))))))
              offset)))))


(defn- addr->elem-idx
  ^long [item-shape item-strides shape-counts arg]
  (long (let [num-items (count item-shape)]
          (loop [idx (long 0)
                 arg (long arg)
                 offset (long 0)]
            (if (< idx num-items)
              (let [next-stride (long (item-strides idx))
                    next-dim (long (item-shape idx))
                    shape-mult (long (get shape-counts idx))
                    n-items (rem (quot arg next-stride)
                                 next-dim)]
                (recur (unchecked-inc idx)
                       (- arg (* next-stride n-items))
                       (+ offset (* shape-mult n-items))))
              offset)))))


(defn- max-extend-strides
  "Extend strides to match the shape vector length by assuming data
  is packed."
  [shape strides max-count]
  (let [shape (shape/shape->count-vec shape)
        num-items (count shape)
        max-stride-idx (long
                        (loop [idx 1
                               max-idx 0]
                          (if (< idx num-items)
                            (do
                              (recur (inc idx)
                                     (long (if (> (long (get strides idx))
                                                  (long (get strides max-idx)))
                                             idx
                                             max-idx))))
                            max-idx)))
        stride-val (* (long (get strides max-stride-idx))
                      (long (get shape max-stride-idx)))]
    (->> (concat (repeat (- (long max-count) (count strides))
                         stride-val)
                 strides)
         vec)))

(defn ->reverse-data
  "Lots of algorithms (elem-idx->addr) require the shape and strides
to be reversed for the most efficient implementation."
  [{:keys [shape strides]} max-shape]
  (let [max-shape-count (count max-shape)
        rev-shape (->> (concat (reverse shape)
                               (repeat 1))
                       (take max-shape-count)
                       vec)
        rev-strides (->> (max-extend-strides shape strides max-shape-count)
                         reverse
                         vec)]
    {:reverse-shape rev-shape
     :reverse-strides rev-strides}))


(defn left-pad-ones
  [shape-vec max-shape-vec]
  (->> (concat (repeat (- (count max-shape-vec)
                          (count shape-vec))
                       1)
               shape-vec)))


(defn dimension-seq->max-shape
  "Given a sequence of dimensions return a map of:
{:max-shape - the maximum dim across shapes for all dims
 :dimensions -  new dimensions with their shape 1-extended to be equal lengths
     and their strides max-extended to be the same length as the new shape."
  [& args]
  (when-not-error (every? #(= (count (:shape %))
                              (count (:strides %)))
                          args)
    "Some dimensions have different shape and stride counts"
    {:args (vec args)})
  (let [shapes (map :shape args)
        strides (map :strides args)
        max-count (long (apply max 0 (map count shapes)))
        ;;Max extend strides that are too small.
        strides (map (fn [shp stride]
                       (max-extend-strides shp stride max-count))
                     shapes strides)
        ;;One extend shapes that are too small
        shapes (map (fn [shp]
                      (->> (concat (repeat (- max-count (count shp)) 1)
                                   shp)
                           vec))
                    shapes)]
    {:max-shape (vec (apply map (fn [& args]
                                  (apply max 0 args))
                            (map shape/shape->count-vec shapes)))
     :dimensions (mapv #(hash-map :shape %1 :strides %2) shapes strides)}))


(defn minimize
  "Make the dimensions of smaller rank by doing some minimization -
a. If the dimension is 1, strip it and associated stride.
b. Combine densely-packed dimensions (not as simple)."
  [dimensions]
  (let [stripped (->> (mapv vector
                            (-> (:shape dimensions)
                                shape/shape->count-vec)
                            (:strides dimensions))
                      (remove (fn [[shp str]]
                                (= 1 (long shp)))))]
    (if (= 0 (count stripped))
      {:shape [1] :strides [1]}
      (let [reverse-stripped (reverse stripped)
            reverse-stripped (reduce
                              (fn [reverse-stripped [[cur-shp cur-stride]
                                                     [last-shp last-stride]]]
                                ;;If the dimension is direct and the stride lines up.
                                (if (= (long cur-stride)
                                       (* (long last-shp) (long last-stride)))
                                  (let [[str-shp str-str] (last reverse-stripped)]
                                    (vec (concat (drop-last reverse-stripped)
                                                 [[(* (long str-shp) (long cur-shp))
                                                   str-str]])))
                                  (conj reverse-stripped [cur-shp cur-stride])))
                              [(first reverse-stripped)]
                              (map vector (rest reverse-stripped) reverse-stripped))
            stripped (reversev reverse-stripped)]
       {:shape (mapv first stripped)
        :strides (mapv second stripped)}))))


(defn in-place-reshape
  "Return new dimensions that correspond to an in-place reshape.  This is a very
  difficult algorithm to get correct as it needs to take into account changing strides
  and dense vs non-dense dimensions."
  [existing-dims shape]
  (let [new-dims (dimensions shape)]
    (when-not-error (<= (ecount new-dims)
                        (ecount existing-dims))
      "Reshaped dimensions are larger than tensor"
      {:tensor-ecount (ecount existing-dims)
       :reshape-ecount (ecount new-dims)})
    (when-not-error (direct? dimensions)
      "Dimensions must be direct for in-place-reshape."
      {:dimensions existing-dims})
    (cond
      ;; a dense brick is easiest case, regardless of
      ;; dimensionality.
      (and (access-increasing? existing-dims)
           (dense? existing-dims))
      {:shape shape
       :strides (extend-strides shape [])}
      ;;Padding creates islands of dense behavior.  We cannot reshape across islands.
      (access-increasing? existing-dims)
      (let [existing-dims (minimize existing-dims)
            existing-rev-shape (reversev (get existing-dims :shape))
            existing-rev-strides (reversev (get existing-dims :strides))
            ;;Find out where there are is padding added.  We cannot combine
            ;;indexes across non-packed boundaries.
            existing-info (mapv vector
                                existing-rev-shape
                                existing-rev-strides)
            new-shape-count (count shape)
            old-shape-count (count existing-info)
            max-old-idx (- old-shape-count 1)
            reverse-shape (reversev shape)
            ;;Index through new shape fitting new shape into old shape.  Each
            ;;time it fits you get a new stride based on the existing shape's
            ;;stride and your previous stride.
            rev-new-strides
            (loop [new-idx 0
                   old-idx 0
                   new-shape reverse-shape
                   existing-info existing-info
                   rev-new-strides []]
              (if (< new-idx new-shape-count)
                (let [[old-dim old-stride old-packed?] (get existing-info
                                                            (min old-idx
                                                                 max-old-idx))
                      new-dim (long (get new-shape new-idx))
                      old-dim (long old-dim)
                      old-stride (long old-stride)]
                  (when-not-error (or (< old-idx old-shape-count)
                                      (= 1 new-dim))
                    "Ran out of old shape dimensions"
                    {:old-idx old-idx
                     :existing-info existing-info
                     :rev-new-strides rev-new-strides
                     :new-dim new-dim})
                  (cond
                    (= 1 new-dim)
                    (recur (inc new-idx) old-idx
                           new-shape existing-info
                           (conj rev-new-strides
                                 (* (long (or (last rev-new-strides) 1))
                                    (long (or (get reverse-shape (dec new-idx))
                                              1)))))
                    (= old-dim new-dim)
                    (recur (inc new-idx) (inc old-idx)
                           new-shape existing-info
                           (conj rev-new-strides old-stride))
                    (< old-dim new-dim)
                    ;;Due to minimization, this is always an error
                    (throw (ex-info "Cannot combine dimensions across padded boundaries"
                                    {:old-dim old-dim
                                     :new-dim new-dim}))
                    (> old-dim new-dim)
                    (do
                      (when-not-error (= 0 (rem old-dim new-dim))
                        "New dimension not commensurate with old dimension"
                        {:old-dim old-dim
                         :new-dim new-dim})
                      (recur (inc new-idx) old-idx
                             new-shape (assoc existing-info
                                              old-idx [(quot old-dim new-dim)
                                                       (* old-stride new-dim)])
                             (conj rev-new-strides old-stride)))))
                rev-new-strides))]
        {:shape shape
         :strides (extend-strides shape (reversev rev-new-strides))})
      :else
      (throw (ex-info "Cannot in-place-reshape transposed or indirect dimensions"
                      {})))))


(defn transpose
  "Transpose the dimensions.  Returns a new dimensions that will access memory in a
  transposed order.
  Dimension 0 is the leftmost (greatest) dimension:

  (transpose tens (range (count (shape tens))))

  is the identity operation."
  [{:keys [shape strides]} reorder-vec]
  (when-not-error (= (count (distinct reorder-vec))
                     (count shape))
    "Every dimension must be represented in the reorder vector"
    {:shape shape
     :reorder-vec reorder-vec})
  (let [shape (mapv #(get shape %) reorder-vec)
        strides (mapv #(get strides %) reorder-vec)]
    {:shape shape
     :strides strides}))



(defn select
  "Expanded implementation of the core.matrix select function call.  Each dimension must
  have an entry and each entry may be:
:all (identity)
:lla (reverse)
persistent-vector: [0 1 2 3 4 4 5] (not supported by all backends)
map: {:type [:+ :-]
      :min-item 0
      :max-item 50}
  Monotonically increasing/decreasing bounded (inclusive) sequences

tensor : int32, dense vector only.  Not supported by all backends.

;;Some examples
https://cloojure.github.io/doc/core.matrix/clojure.core.matrix.html#var-select"
  [dimensions & args]
  (let [data-shp (shape dimensions)]
    (when-not-error (= (count data-shp)
                       (count args))
      "arg count must match shape count"
      {:shape data-shp
       :args (vec args)})
    (let [{:keys [shape strides]} dimensions

          shape (map dims-select/apply-select-arg-to-dimension shape args)
          {shape :dimension-seq
           strides :strides
           offset :offset
           buffer-length :length} (dims-select/dimensions->simpified-dimensions
                                   shape strides)]
      {:dimensions {:shape shape
                    :strides strides}
       :elem-offset offset
       :buffer-length buffer-length})))


(defn dimensions->column-stride
  ^long [{:keys [shape strides]}]
  (long
   (let [dim-count (count strides)]
     (if (> dim-count 1)
       ;;get the second from the last stride
       (get strides (- dim-count 2))
       ;;Get the dimension count
       (get shape 0 1)))))


(defn trans-2d-shape
  [trans-a? dims]
  (let [[rows cols] (->2d-shape dims)]
    (if trans-a?
      [cols rows]
      [rows cols])))


(defn matrix-column-stride
  "Returns the larger of the 2 strides"
  ^long [{:keys [shape strides] :as dims}]
  (when-not-error (= 2 (count shape))
    "Not a matrix" {:dimensions dims})
  (apply max strides))


(defn matrix-element-stride
  ^long [{:keys [shape strides] :as dims}]
  (when-not-error (= 2 (count shape))
    "Not a matrix" {:dimensions dims})
  (apply min strides))
