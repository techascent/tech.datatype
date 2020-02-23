(ns tech.v2.tensor.dimensions.global-to-local
  "Given a generic description object, return an interface that can efficiently
  transform indexes in global coordinates mapped to local coordinates."
  (:require [tech.v2.tensor.dimensions.shape :as shape]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.functional :as dtype-fn]
            [primitive-math :as pmath]))

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


(defn dims->shape-data
  [{:keys [shape strides offsets max-shape] :as dims}]
  (let [direct? (shape/direct-shape? shape)
        shape-ecount (long (apply * max-shape))
        offsets? (not (every? #(== 0 (long %)) offsets))
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


(defn- number-or-reader
  [shape-entry]
  (if (number? shape-entry)
    shape-entry
    (dtype/->reader shape-entry :int64)))


(defn reduce-dimensionality
  "Make a smaller equivalent shape in terms of row-major addressing
  from the given shape."
  ([{:keys [shape strides offsets max-shape] :as dims}
    offsets?
    broadcast?]
   (let [shape (object-array shape)
         strides (long-array strides)
         offsets (long-array offsets)
         max-shape (long-array max-shape)
         breaks (find-breaks shape strides max-shape offsets)]
     {:shape (object-array (map #(if (== 1 (count %))
                                   (->
                                    (aget shape (long (first %)))
                                    (number-or-reader))
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
      :max-shape (when broadcast?
                   (long-array (map
                                #(reduce * (map (fn [idx] (aget max-shape (long idx)))
                                                %))
                                breaks)))}))
  ([{:keys [shape strides offsets max-shape] :as dims}]
   (reduce-dimensionality dims true true)))


(defn- ast-symbol-access
  [symbol-stem dim-idx]
  (symbol (format "%s-%d" symbol-stem dim-idx)))


(defn- ast-array-access
  [symbol-stem dim-idx-sym]
  `(~'aget ~symbol-stem ~dim-idx-sym))


(defn- make-symbol
  [symbol-stem dim-idx]
  (if (number? dim-idx)
    (ast-symbol-access symbol-stem dim-idx)
    (ast-array-access (symbol symbol-stem) dim-idx)))


(defn elemwise-ast
  [dim-idx direct? offsets? broadcast?
   trivial-stride?
   most-rapidly-changing-index?
   least-rapidly-changing-index?]
  (let [shape (make-symbol "shape" dim-idx)
        stride (make-symbol "stride" dim-idx)
        offset (make-symbol "offset" dim-idx)
        max-shape-stride (make-symbol "max-shape-stride" dim-idx)]
    (let [idx (if most-rapidly-changing-index?
                `~'idx
                `(~'quot ~'idx ~max-shape-stride))
          offset-idx (if offsets?
                       `(~'+ ~idx ~offset)
                       `~idx)
          shape-ecount (if direct?
                           `~shape
                           `(.lsize ~shape))
          idx-bcast (if (or offsets? broadcast? (not least-rapidly-changing-index?))
                      `(~'rem ~offset-idx ~shape-ecount)
                      `~offset-idx)
          elem-idx (if direct?
                     `~idx-bcast
                     `(.read ~shape ~idx-bcast))]
      (if trivial-stride?
        `~elem-idx
        `(~'* ~elem-idx ~stride)))))


(defn global->local-ast
  [n-dims direct-vec offsets? broadcast?
   trivial-last-stride?]
  (if (= n-dims 1)
    (elemwise-ast 0 (direct-vec 0) offsets? broadcast?
                  trivial-last-stride? true true)
    (let [n-dims (long n-dims)
          n-dims-dec (dec n-dims)]
      {:key {:n-dims n-dims
             :direct-vec direct-vec
             :offsets? offsets?
             :broadcast? broadcast?
             :trivial-last-stride? trivial-last-stride?}
       :ast
       (->> (range n-dims)
            (map (fn [dim-idx]
                   (let [dim-idx (long dim-idx)
                         least-rapidly-changing-index? (== dim-idx 0)
                         most-rapidly-changing-index? (== dim-idx n-dims-dec)
                         trivial-stride? (and most-rapidly-changing-index?
                                              trivial-last-stride?)]
                     (elemwise-ast dim-idx (direct-vec dim-idx) offsets? broadcast?
                                   trivial-stride? most-rapidly-changing-index?
                                   least-rapidly-changing-index?))))
            (apply list '+))})))


(defn dims->global->local-equation
  [{:keys [shape strides offsets max-shape] :as dims} & [shape-data]]
  (let [shape-data (or shape-data (dims->shape-data dims))
        n-dims (long (:n-dims shape-data))
        n-dims-dec (dec n-dims)
        offsets? (:offsets? shape-data)
        broadcast? (:broadcast? shape-data)
        reduced-dims (reduce-dimensionality dims offsets? broadcast?)
        ^objects reduced-shape (:shape reduced-dims)
        direct-vec (mapv number? reduced-shape)
        ^longs reduced-strides (:strides reduced-dims)
        ^longs reduced-offsets (when offsets? (:offsets reduced-dims))
        ^longs reduced-max-shape (when broadcast? (:max-shape reduced-dims))
        n-reduced-dims (alength reduced-shape)]
    (assoc
     (global->local-ast n-reduced-dims direct-vec offsets? broadcast?
                        (== 1 (aget reduced-strides (dec n-reduced-dims))))
     :reduced-dims reduced-dims)))
