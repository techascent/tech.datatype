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
  the running time of elementwise global->local is dependent upon the
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
               next-shape-entry (aget shape idx)
               entry-number? (number? next-shape-entry)
               next-break
               (if (and (pmath/== (aget strides idx)
                                  next-stride)
                        (and last-entry-number?
                             (pmath/== (long last-shape-entry)
                                       (aget max-shape (inc idx))))
                        (and entry-number?
                             (pmath/== (long next-shape-entry)
                                       (aget max-shape idx)))
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
                                   (aget shape (long (first %)))
                                   (apply * (map (fn [idx]
                                                   (aget shape (long idx)))
                                                 %)))
                                breaks))
      :strides (long-array (map
                            #(reduce * (map (fn [idx] (aget strides (long idx)))
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
  ([[{:keys [shape strides offsets max-shape] :as dims}]]
   (reduce-dimensionality dims true true)))



(defn dims->global->local-equation
  [{:keys [shape strides offsets max-shape] :as dims} & [shape-data]]
  (let [shape-data (or shape-data (dims->shape-data dims))
        n-dims (long (:n-dims shape-data))
        n-dims-dec (dec n-dims)
        direct? (:direct? shape-data)
        offsets? (:offsets? shape-data)
        broadcast? (:broadcast? shape-data)
        reduced-dims (reduce-dimensionality dims offsets? broadcast?)
        ^objects shape (:shape reduced-dims)
        ^longs strides (:strides reduced-dims)
        ^longs offsets (when offsets? (:offsets reduced-dims))
        ^longs max-shape (if broadcast?
                           (:max-shape reduced-dims)
                           (long-array (shape/shape-vec shape)))
        ^longs max-shape-strides (if broadcast?
                                   (shape-ary->strides max-shape)
                                   strides)
        shape-vec (:shape-vec shape-data)]
    (cond
      (and direct?)
      )
    (->> (range n-dims)
         (mapcat (fn [dim-idx]
                   (let [dim-idx (long dim-idx)
                         shape-sym (symbol (str "shape-" dim-idx))
                         stride-sym (symbol (str "stride-" dim-idx))
                         offset-sym (symbol (str "offset-" dim-idx))
                         specific-shape (long (shape-vec (- n-dims-dec dim-idx)))
                         specific-max-shape (long (max-shape (- n-dims-dec dim-idx)))
                         specific-offset (long (offsets (- n-dims-dec dim-idx)))
                         idx-eqn (if (== specific-offset 0)
                                   'idx
                                   (list '+ 'idx specific-offset))
                         local-idx-eqn (if (== specific-shape specific-max-shape)
                                         idx-eqn
                                         (list 'rem idx-eqn specific-shape))]
                     (concat
                      [(list '+= 'retval (list '* (list 'quot local-idx-eqn shape-sym)
                                               stride-sym))]
                      (when-not (= dim-idx ()))
                      (list 'assign 'idx (list 'rem ))))
                   )))
    )
  )
