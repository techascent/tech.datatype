(ns tech.v2.tensor.dimensions.global-to-local
  "Given a generic description object, return an interface that can efficiently
  transform indexes in global coordinates mapped to local coordinates."
  (:require [tech.v2.tensor.dimensions.shape :as shape]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dtype-fn]))


(defn dims->shape-data
  [{:keys [shape strides offsets max-shape]} dims]
  (let [direct? (shape/direct-shape? shape)
        shape-ecount (long (apply * max-shape))
        dense? (if direct?
                 (= shape-ecount
                    (buffer-ecount dims))
                 false)
        increasing? (if direct?
                      (and (apply >= strides)
                           (every? #(= 0 %) offsets))
                      false)
        vec-shape (if direct?
                    shape
                    (shape/shape->count-vec shape))
        broadcast? (not= vec-shape max-shape)]
    {:direct? direct?
     :dense? dense?
     :increasing? increasing?
     :broadcast? broadcast?
     :offsets? (boolean (seq offsets))
     :n-dims (count shape)
     :shape-ecount shape-ecount
     :vec-shape vec-shape}))


(defn shape-data->signature
  [shape-data]
  (dissoc shape-data :shape-ecount :vec-shape))


(defn get-elem-dims-global->local-ast
  "Return an ast of the form:
  Reads data in row-major form.
  {:signature - ast signature this method will work on.
   :methods - method definitions - read, read2d, read3d. May be sparse.
   :constructor - variable definitions used by method definitions & initialization
                  used by method definitions.  Takes a dimension object.
  }
  "
  [dims]
  (let [{:keys [shape strides offsets max-shape]} dims
        {:keys [direct? shape-ecount dense? increasing?
                n-dims vec-shape broadcase?] :as shape-data} (dims->shape-data dims)
        n-dims (long n-dims)
        shape-ecount (long shape-ecount)

        ;;Any indirect addressing?
        min-shape (drop-while #(= 1 %) shape)
        local-ec shape-ecount
        n-elems (shape/ecount max-shape)
        stride-reader (typecast/datatype->reader :int32 (int-array strides))
        shape-reader (typecast/datatype->reader :int32 vec-shape)
        n-dims (count shape)
        ;;The 2d case is important and special.
        stride-0 (int (.read stride-reader 0))
        stride-1 (int (if (>= n-dims 2)
                        (.read stride-reader 1)
                        0))
        stride-2 (int (if (>= n-dims 3)
                        (.read stride-reader 2)
                        0))
        shape-0 (int (first vec-shape))
        shape-1 (int (if (>= n-dims 2)
                       (second vec-shape)
                       1))
        shape-2 (int (if (>= n-dims 3)
                       (nth vec-shape 2)
                       1))]
    ;;If the shape is all integers (no indirect indexing)
    (if direct?
      {:signature (shape-data->signature shape-data)
       :methods
       (merge {:read (->> '(loop [idx (long 0)
                                  arg (long arg)
                                  offset (long 0)]
                             (if (< idx num-items)
                               (let [next-max (aget rev-max-shape idx)
                                     next-stride (aget rev-strides idx)
                                     ^LongReader next-dim-entry (rev-shape idx)
                                     next-dim (.lsize next-dim-entry)
                                     next-offset (aget rev-offsets idx)
                                     shape-idx (rem (+ arg next-offset) next-dim)]
                                 (recur (inc idx)
                                        (quot arg next-max)
                                        (+ offset (* next-stride
                                                     (.read next-dim-entry shape-idx)))))))) }
              )})
    (cond
      ;;Special case for indexes that increase monotonically
      (and direct?
           (not broadcast?)
           dense?
           increasing?)
      (impl-idx-reader n-elems idx
                       (+ (* (int row) stride-0)
                          (* (int col) stride-1))
                       (+ (* (int row) stride-0)
                          (* (int col) stride-1)
                          (* (int chan) stride-2))
                       (dense-integer-dot-product indexes stride-reader))
      ;;Special case for broadcasting a vector across an image (like applying bias).
      (and direct?
           (= (ecount dims)
              (apply max vec-shape))
           dense?
           increasing?)
      (let [ec-idx (long
                    (->> (map-indexed vector (left-pad-ones
                                              vec-shape max-shape))
                         (filter #(= local-ec (second %)))
                         (ffirst)))
            broadcast-amt (long (apply * 1 (drop (+ 1 ec-idx) max-shape)))]
        (impl-idx-reader n-elems
                         (rem (quot idx broadcast-amt)
                              local-ec)
                         (+ (* (rem row shape-0) stride-0)
                            (* (rem col shape-1) stride-1))
                         (+ (* (rem row shape-0) stride-0)
                            (* (rem col shape-1) stride-1)
                            (* (rem chan shape-2) stride-2))
                         (dense-integer-dot-product
                          stride-reader
                          (binary-op/binary-iterable-map
                           {:datatype :int32}
                           rem-int-op
                           indexes
                           shape-reader))))

      ;;Special case where the entire shape is being broadcast from an
      ;;outer dimension. [2 2] broadcast into [4 2 2].
      (and direct?
           dense?
           increasing?
           (= min-shape
              (take-last (count min-shape) max-shape)))
      (impl-idx-reader n-elems
                       (rem idx local-ec)
                       (+ (* (rem row shape-0) stride-0)
                          (* (rem col shape-1) stride-1))
                       (+ (* (rem row shape-0) stride-0)
                          (* (rem col shape-1) stride-1)
                          (* (rem chan shape-2) stride-2))
                       (dense-integer-dot-product
                        stride-reader
                        (binary-op/binary-iterable-map
                         {:datatype :int32}
                         rem-int-op
                         indexes
                         shape-reader)))
      :else
      (let [{:keys [reverse-shape reverse-strides]}
            (->reverse-data dims)
            rev-max-shape (int-array (utils/reversev max-shape))
            reverse-offsets (if-let [item-offsets (:offsets dims)]
                              (utils/reversev item-offsets)
                              (const-reader/make-const-reader 0 :int32
                                                              (count reverse-shape)))]
        ;;General case when direct shape
        (if direct?
          (let [rev-shape (int-array reverse-shape)
                rev-strides (int-array reverse-strides)
                rev-offsets (int-array reverse-offsets)
                shape-0 (aget rev-shape 0)
                shape-1 (int (if (>= n-dims 2)
                               (aget rev-shape 1)
                               1))
                shape-2 (int (if (>= n-dims 3)
                               (aget rev-shape 2)
                               1))
                offset-0 (aget rev-offsets 0)
                offset-1 (int (if (>= n-dims 2)
                                (aget rev-offsets 1)
                                0))
                offset-2 (int (if (>= n-dims 3)
                                (aget rev-offsets 2)
                                0))
                stride-0 (aget rev-strides 0)
                stride-1 (int (if (>= n-dims 2)
                                (aget rev-strides 1)
                                0))
                stride-2 (int (if (>= n-dims 3)
                                (aget rev-strides 2)
                                0))]
            (if (and (not broadcast?)
                     (<= n-dims 2)
                     access-increasing?)
              ;;Common cases with offsetting
              (if (= 2 n-dims)
                (impl-idx-reader
                 n-elems
                 (let [first-elem (rem (unchecked-add idx offset-0) shape-0)
                       next-elem (rem (unchecked-add (quot idx shape-0) offset-1)
                                      shape-1)]
                   (unchecked-add (unchecked-multiply first-elem stride-0)
                                  (unchecked-multiply  next-elem stride-1)))
                 ;;We reversed them, so the indexes may be seem odd.
                 (+ (* (rem (unchecked-add col offset-0) shape-0) stride-0)
                    (* (rem (unchecked-add row offset-1) shape-1) stride-1))
                 (+ (* (rem (unchecked-add chan offset-0) shape-0) stride-0)
                    (* (rem (unchecked-add col offset-1) shape-1) stride-1)
                    (* (rem (unchecked-add row offset-2) shape-2) stride-2))
                 (let [iter (typecast/datatype->iter :int64 indexes)]
                   (.read2d reader (.nextLong iter) (.nextLong iter))))
                (impl-idx-reader
                 n-elems
                 (let [first-elem (rem (+ idx offset-0) shape-0)]
                   (* first-elem stride-0))
                 (* (rem (unchecked-add col offset-0) shape-0) stride-0)
                 (* (rem (unchecked-add chan offset-0) shape-0) stride-0)
                 (.read2d reader 0 (-> (typecast/datatype->iter :int64 indexes)
                                       (.nextLong)))))
              ;;Broadcasting with offsets but direct shape general case.
              (impl-idx-reader
               n-elems
               (let [arg idx]
                 (loop [idx (long 0)
                        arg (long arg)
                        offset (long 0)]
                   (if (< idx n-dims)
                     (let [next-max (aget rev-max-shape idx)
                           next-stride (aget rev-strides idx)
                           next-dim (aget rev-shape idx)
                           next-off (aget rev-offsets idx)
                           shape-idx (rem (+ arg next-off) next-dim)]
                       (recur (inc idx)
                              (quot arg next-max)
                              (+ offset (* next-stride shape-idx))))
                     offset)))
               (+ (* (rem (unchecked-add col offset-0) shape-0) stride-0)
                  (* (rem (unchecked-add row offset-1) shape-1) stride-1))
               (+ (* (rem (unchecked-add chan offset-0) shape-0) stride-0)
                  (* (rem (unchecked-add col offset-1) shape-1) stride-1)
                  (* (rem (unchecked-add row offset-2) shape-2) stride-2))

               (let [indexes (binary-op/binary-iterable-map
                              {:datatype :int32}
                              add-int-op
                              indexes
                              offsets)]
                 (dense-integer-dot-product
                  stride-reader
                  (binary-op/binary-iterable-map
                   {:datatype :int32}
                   rem-int-op
                   indexes
                   shape-reader))))))
          ;;Totally general case to encapsulate all the variations including indexed
          ;;dimensions.
          (let [^List reverse-shape
                (mapv (fn [item]
                        (cond
                          (number? item)
                          (let [item (long item)]
                            (reify LongReader
                              (lsize [rdr] item)
                              (read [rdr idx] idx)))
                          (map? item)
                          (dtype/->reader
                           (shape/classified-sequence->sequence item)
                           :int64)
                          :else
                          (dtype/->reader item :int64)))
                      reverse-shape)
                reverse-max-shape (typecast/datatype->reader :int32
                                                             (int-array rev-max-shape))
                max-strides (extend-strides max-shape)
                max-stride-0 (int (first max-strides))
                max-stride-1 (int (if (>= n-dims 2)
                                    (second max-strides)
                                    0))
                max-stride-2 (int (if (>= n-dims 3)
                                    (nth max-strides 2)
                                    0))
                reverse-strides (dtype/make-container :java-array :int64
                                                      reverse-strides)
                reverse-offsets (dtype/make-container :java-array :int64
                                                      reverse-offsets)
                reverse-max-shape (dtype/make-container :java-array :int64
                                                        reverse-max-shape)]
            (impl-idx-reader
             n-elems
             (elem-idx->addr reverse-shape
                             reverse-strides
                             reverse-offsets
                             reverse-max-shape
                             idx)
             (.read reader
                    (+ (* row max-stride-0)
                       (* col max-stride-1)))
             (.read reader
                    (+ (* row max-stride-0)
                       (* col max-stride-1)
                       (* chan max-stride-2)))
             (.read reader (int (dense-integer-dot-product indexes
                                                           max-strides))))))))))
