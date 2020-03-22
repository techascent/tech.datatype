(ns tech.v2.tensor.dimensions
  "Compute tensors dimensions control the shape and stride of the tensor along with
  offsetting into the actual data buffer.  This allows multiple backends to share a
  single implementation of a system that will allow transpose, reshape, etc. assuming
  the backend correctly interprets the shape and stride of the dimension objects.

  Shape vectors may have an index buffer in them at a specific dimension instead of a
  number.  This means that that dimension should be indexed indirectly.  If a shape has
  any index buffers then it is considered an indirect shape."
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.tensor.dimensions.select :as dims-select]
            [tech.v2.tensor.dimensions.analytics :as dims-analytics]
            [tech.v2.tensor.dimensions.shape :as shape]
            [tech.v2.tensor.dimensions.global-to-local :as gtol]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.index-algebra :as idx-alg]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.boolean-op :as boolean-op]
            [tech.v2.datatype.readers.const :as const-reader]
            [tech.v2.datatype.readers.indexed :as indexed-reader]
            [tech.v2.datatype.argsort :as argsort]
            [tech.v2.tensor.utils
             :refer [when-not-error reversev]
             :as utils])
  (:import [tech.v2.datatype
            IndexingSystem$Backward
            IntReader
            LongReader]
           [java.util List Map]
           [it.unimi.dsi.fastutil.ints IntArrayList]
           [clojure.lang IDeref]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(declare create-dimension-transforms)


(defrecord Dimensions [^List shape ;;list of things.  Not longs.
                       ^List strides ;;list of longs
                       ^long n-dims ;;(count shape)
                       ^List shape-ecounts ;;list of longs
                       ^List shape-ecount-strides ;;list of naive strides if the shape-ecounts were the shape.
                       ^long overall-ecount ;;ecount of the dimensions
                       ;;Not always detectable from the shape, if it is this is the smallest dense buffer
                       ;;that could contain data described with these dimensions.  May be nil or long.
                       buffer-ecount
                       ;;the result of the dims-analytics reduction process.
                       reduced-dims
                       ^boolean broadcast? ;; true any shape member is broadcast
                       ^boolean offsets? ;; true if any shape member has offsets
                       ^boolean shape-direct? ;; true if all shape entries are numbers
                       ;;True if all shape entries are numbers and based on those numbers
                       ;;the strides indicate data is packed.
                       ^boolean direct?
                       ;;Are the strides ordered from greatest to least?
                       ^boolean access-increasing?
                       ;;delay of global->local transformation
                       global->local
                       ;;delay of global->local transformation
                       local->global]
  dtype-proto/PShape
  (shape [item] shape-ecounts)
  dtype-proto/PCountable
  (ecount [item] overall-ecount))


(defn dimensions
  "Dimensions contain information about how to map logical global indexes to local
  buffer addresses."
  ([shape strides]
   (let [n-dims (count shape)
         _ (when-not (== n-dims (count strides))
             (throw (Exception. "shape/strides ecount mismatch")))
         shape-direct? (every? idx-alg/direct? shape)
         shape-ecounts (if shape-direct?
                         shape
                         (mapv shape/shape-entry->count shape))
         offsets? (boolean (and (not shape-direct?)
                                (some idx-alg/offset? shape)))
         broadcast? (boolean (and (not shape-direct?)
                                  (some idx-alg/broadcast? shape)))
         access-increasing? (boolean (apply >= strides))
         ^List strides strides
         ^List shape shape
         direct? (boolean
                  (and shape-direct?
                       (== 1 (long (.get strides 0)))
                       (loop [idx 1]
                         (cond
                           (== idx n-dims) true
                           (not= (long (.get strides idx))
                                 (let [didx (unchecked-dec idx)]
                                   (* (long (.get strides didx))
                                      (long (.get shape didx)))))
                           false
                           :else
                           (recur (unchecked-inc idx))))))
         overall-ecount (long (apply * shape-ecounts))
         buffer-ecount (if direct?
                         overall-ecount
                         (loop [idx 0
                                loop-valid? true
                                max-stride 0
                                overall-ecount nil]
                           (if (and loop-valid?
                                    (< idx n-dims))
                             (let [stride-val (long (.get strides idx))
                                   shape-val (.get shape idx)
                                   loop-valid? (or (number? shape-val)
                                                   (dtype-proto/has-constant-time-min-max? shape-val))
                                   max-stride (max stride-val max-stride)]
                               (recur (unchecked-inc idx)
                                      loop-valid?
                                      max-stride
                                      (long (if (and (== stride-val max-stride)
                                                     loop-valid?)
                                              (* stride-val (unchecked-inc
                                                             (long (dtype-proto/constant-time-max
                                                                    shape-val))))
                                              -1))))
                             (when loop-valid? overall-ecount))))
         shape-ecount-strides (if direct?
                                strides
                                (dims-analytics/shape-ary->strides shape-ecounts))
         reduced-dims (dims-analytics/reduce-dimensionality
                       {:shape shape
                        :strides strides
                        :shape-ecounts shape-ecounts})
         half-retval (->Dimensions shape
                                   strides
                                   n-dims
                                   shape-ecounts
                                   shape-ecount-strides
                                   overall-ecount
                                   reduced-dims
                                   broadcast?
                                   offsets?
                                   shape-direct?
                                   direct?
                                   access-increasing?
                                   nil
                                   nil)]
     (create-dimension-transforms half-retval)))
  ([shape]
   (let [n-dims (count shape)
         ^List shape shape
         strides (dims-analytics/shape-ary->strides shape)
         shape-ecounts shape
         shape-entry-ecounts strides
         shape-direct? true
         offsets? false
         broadcast? false
         direct? true
         access-increasing? true
         overall-ecount (long (* (long (.get shape 0))
                                 (long (.get strides 0))))
         reduced-dims (dims-analytics/simple-direct-reduced-dims
                       overall-ecount)
         half-retval (->Dimensions shape strides
                                   shape-ecounts
                                   overall-ecount
                                   overall-ecount
                                   reduced-dims
                                   broadcast?
                                   offsets?
                                   shape-direct?
                                   direct?
                                   access-increasing?
                                   nil
                                   nil)]
     (create-dimension-transforms half-retval))))


(defn ecount
  ^long [{:keys [overall-ecount]}]
  (long overall-ecount))

(defn buffer-ecount
  "What is the necessary ecount for a given buffer.  Maybe  nil if this could not be detected
  from the dimension arguments."
  [{:keys [buffer-ecount]}]
  buffer-ecount)


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
  [{:keys [max-shape]}]
  max-shape)


(defn strides
  [{:keys [strides]}]
  strides)


(defn direct?
  [{:keys [direct?]}]
  direct?)


(defn indirect?
  [dims]
  (not (direct? dims)))


(defn access-increasing?
  "Are these dimensions setup such a naive seq through the data will be accessing memory
  in order.  This is necessary for external library interfaces (blas, cudnn).  An
  example would be after any nontrivial transpose that is not made concrete (copied)
  this condition will not hold."
  [{:keys [access-increasing?]}]
  access-increasing?)


(defn ->most-rapidly-changing-dimension
  "Get the size of the most rapidly changing dimension"
  ^long [{:keys [shape-ecounts]}]
  (last shape-ecounts))


(defn ->least-rapidly-changing-dimension
  "Get the size of the least rapidly changing dimension"
  ^long [{:keys [shape-ecounts]}]
  (first shape-ecounts))


(defn local-address->local-shape
  "Shape and strides are not transposed.  Returns
  [valid? local-shape-as-list]"
  [shape offsets strides shape-mins addr]
  (let [strides (typecast/datatype->reader :int32 strides)
        shape (typecast/datatype->reader :int32 shape)
        offsets (typecast/datatype->reader :int32 offsets)
        addr (int addr)
        n-elems (.lsize strides)
        retval (dtype/make-container :list :int32 0)
        retval-mut (typecast/datatype->mutable :int32 retval)]
    (loop [idx 0
           addr addr]
      (if (< idx n-elems)
        (let [local-stride (.read strides idx)
              shape-idx (quot addr local-stride)
              local-shape (.read shape idx)]
          (if (and (< shape-idx local-shape)
                   (>= shape-idx (long (shape-mins idx))))
            (let [shape-idx (- shape-idx (.read offsets idx))
                  shape-idx (if (< shape-idx 0)
                              (+ shape-idx local-shape)
                              shape-idx)]
              (.append retval-mut (int shape-idx))
              (recur (unchecked-inc idx) (rem addr local-stride)))
            (recur n-elems -1)))
        (when (= 0 addr)
          retval)))))


(defn dense-integer-dot-product
  ^long [^IntReader lhs ^IntReader rhs]
  (let [n-elems (.lsize lhs)]
    (loop [idx 0
           sum 0]
      (if (< idx n-elems)
        (recur (unchecked-inc idx)
               (+ sum (* (.read lhs idx)
                         (.read rhs idx))))
        sum))))


(defn ->global->local
  ^LongReader [dims]
  @(:global->local dims))


(defn ->local->global
  ^IndexingSystem$Backward [dims]
  @(:local->global dims))


(defn get-elem-dims-local->global
  "Harder translation than above.  May return nil in the case where the inverse
  operation hasn't yet been derived.  In this case, the best you can do is a O(N)
  iteration similar to dense math."
  ^IndexingSystem$Backward
  [dims]
  (if (:direct? dims)
    (reify IndexingSystem$Backward
      (localToGlobal [item local-idx] local-idx))
    ;;TODO - rebuild faster and less memory intensive paths for this.
    ;;This will just make the problem go away and allows rapid indexing.
    (if (< (dtype/ecount dims)
           Integer/MAX_VALUE)
      (let [group-map (dfn/arggroup-by-int identity (->global->local dims))]
        (reify IndexingSystem$Backward
          (localToGlobal [item local-idx]
            (dtype/->reader (get group-map (long local-idx))
                            :int64))))
      (let [group-map (dfn/arggroup-by identity (->global->local dims))]
        (reify IndexingSystem$Backward
          (localToGlobal [item local-idx]
            (get group-map (long local-idx))))))))


(defn create-dimension-transforms [dims]
  (assoc dims
         :global->local (delay (gtol/dims->global->local dims))
         ;;:global->local (delay (get-elem-dims-global->local dims))
         :local->global (delay (get-elem-dims-local->global dims))))



(defn dimension-seq->max-shape
  "Given a sequence of dimensions return the max shape overall all dimensions."
  [& args]
  (let [shapes (map :shape args)
        max-count (long (apply max 0 (map count shapes)))
        ;;One extend shapes that are too small
        shapes (map (fn [shp]
                      (->> (concat (repeat (- max-count (count shp)) 1)
                                   shp)
                           vec))
                    shapes)]
    (vec (apply map (fn [& args]
                      (apply max 0 args))
                (map shape/shape->count-vec shapes)))))


(defn minimize
  "Make the dimensions of smaller rank by doing some minimization -
a. If the dimension is 1, strip it and associated stride.
b. Combine densely-packed dimensions (not as simple)."
  [dimensions]
  (let [stripped (->> (mapv vector
                            (-> (:shape dimensions)
                                shape/shape->count-vec)
                            (:strides dimensions))
                      (remove (fn [[shp _]]
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
      (dimensions shape)
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
                (let [[old-dim old-stride _old-packed?] (get existing-info
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
        (dimensions shape :strides (extend-strides shape (reversev rev-new-strides))))
      :else
      (throw (ex-info "Cannot in-place-reshape transposed or indirect dimensions"
                      {})))))


(defn transpose
  "Transpose the dimensions.  Returns a new dimensions that will access memory in a
  transposed order.
  Dimension 0 is the leftmost (greatest) dimension:

  (transpose tens (range (count (shape tens))))

  is the identity operation."
  [{:keys [shape offsets strides]} reorder-vec]
  (when-not-error (= (count (distinct reorder-vec))
                     (count shape))
    "Every dimension must be represented in the reorder vector"
    {:shape shape
     :reorder-vec reorder-vec})
  (let [shape (mapv #(dtype/get-value shape %) reorder-vec)
        strides (mapv #(dtype/get-value strides %) reorder-vec)
        offsets (mapv #(dtype/get-value offsets %) reorder-vec)]
    (dimensions shape :strides strides :offsets offsets)))



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
  [dims & args]
  (let [data-shp (shape dims)]
    (when-not-error (= (count data-shp)
                       (count args))
      "arg count must match shape count"
      {:shape data-shp
       :args (vec args)})
    (let [{:keys [shape offsets strides]} dims
          ;;mapv here in order to correctly attribute timings during
          ;;profiling.
          shape (mapv dims-select/apply-select-arg-to-dimension shape args)
          {shape :dimension-seq
           strides :strides
           offsets :offsets
           offset :offset
           buffer-length :length
           :as _simplified-map} (dims-select/dimensions->simpified-dimensions
                                 shape strides offsets)]
      {:dims (dimensions shape :strides strides :offsets offsets)
       :elem-offset offset
       :buffer-length buffer-length})))


(defn rotate
  "Dimensional rotations are applied via offsetting."
  [dims new-offset-vec]
  (if (every? #(= 0 (long %)) new-offset-vec)
    dims
    (let [old-offsets (:offsets dims)
          _ (when-not (= (dtype-base/ecount old-offsets)
                         (dtype-base/ecount new-offset-vec))
              (throw (ex-info "Rotation offset vector count mismatch."
                              {:old-offset-count (dtype-base/ecount old-offsets)
                               :new-offsets-count (dtype-base/ecount new-offset-vec)})))
          new-offsets (mapv (fn [old-offset new-offset dim]
                              (let [potential-new-offset (+ (long old-offset)
                                                            (long new-offset))
                                    dim (long dim)]
                                (if (< potential-new-offset 0)
                                  (+ potential-new-offset
                                     (* (quot (+ (- potential-new-offset)
                                                 (- dim 1))
                                              dim)
                                        dim))
                                  (rem potential-new-offset dim))))
                            old-offsets
                            new-offset-vec
                            (shape/shape->count-vec (:shape dims)))]
      (dimensions (:shape dims)
                  :strides (:strides dims)
                  :offsets new-offsets))))


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


(defn contiguous-shape
  "Starting from the right, return a sequence that counts the total number of contigous
  elements see so far.  Used to decide if contiguous copy routines are worthwhile
  or if just normal parallelized reader copy is fine."
  [{:keys [shape strides]}]
  (loop [rev-shape (reverse shape)
         rev-strides (reverse strides)
         start-elem 1
         retval []]
    (if (and (number? (first rev-shape))
             (= (first rev-strides)
                start-elem))
      (let [n-elems (* (long (first rev-shape))
                       (long (first rev-strides)))]
        (recur (rest rev-shape)
               (rest rev-strides)
               n-elems
               (conj retval n-elems)))
      (seq (reverse retval)))))
