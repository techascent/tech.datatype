(ns tech.tensor.index-system
  (:require [tech.datatype :as dtype]
            [tech.datatype.base :as dtype-base]
            [tech.tensor.dimensions :as dims]
            [tech.tensor.dimensions.shape :as shape]
            [tech.tensor.utils :as utils]
            [tech.datatype.typecast :as typecast]
            [tech.datatype.argsort :as argsort]
            [tech.datatype.reader :as reader]
            [tech.datatype.unary-op :as unary-op]
            [tech.datatype.binary-op :as binary-op]
            [tech.datatype.reduce-op :as reduce-op]
            [tech.datatype.boolean-op :as boolean-op]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.binary-search :as dtype-search])
  (:import [tech.datatype
            IndexingSystem$Forward
            IndexingSystem$Backward
            IntReader]))

(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn get-elem-dims-global->local
  ^IndexingSystem$Forward [dims max-shape]
  ;;Special cases here for speed
  (let [dense? (dims/dense? dims)
        increasing? (dims/access-increasing? dims)
        ;;Any indirect addressing?
        direct? (dims/direct? dims)
        min-shape (drop-while #(= 1 %) (dims/shape dims))
        local-ec (dims/ecount dims)
        max-ec (dtype-base/shape->ecount max-shape)]
    (cond
      ;;Special case for indexes that increase monotonically
      (and direct?
           (= (:shape dims)
              max-shape)
           dense?
           increasing?)
      (reify IndexingSystem$Forward
        (globalToLocal [item idx] idx))
      ;;Special case for broadcasting a vector across an image (like applying bias).
      (and direct?
           (= (dims/ecount dims)
              (apply max (dims/shape dims)))
           dense?
           increasing?)
      (let [ec-idx (long
                    (->> (map-indexed vector (dims/left-pad-ones
                                              (dims/shape dims) max-shape))
                         (filter #(= local-ec (second %)))
                         (ffirst)))
            broadcast-amt (long (apply * 1 (drop (+ 1 ec-idx) max-shape)))]
        (reify IndexingSystem$Forward
          (globalToLocal [item idx]
            (rem (quot idx broadcast-amt)
                 local-ec))))

      ;;Special case where the entire shape is being broadcast from an
      ;;outer dimension. [2 2] broadcast into [4 2 2].
      (and direct?
           dense?
           increasing?
           (= min-shape
              (take-last (count min-shape) max-shape)))
      (reify IndexingSystem$Forward
        (globalToLocal [item arg]
          (rem arg local-ec)))
      :else
      (let [{:keys [reverse-shape reverse-strides]}
            (dims/->reverse-data dims max-shape)
            rev-max-shape (int-array (utils/reversev max-shape))]
        ;;General case when non of the dimensions are indexed themselves.
        (if direct?
          (let [rev-shape (int-array reverse-shape)
                rev-strides (int-array reverse-strides)
                num-items (alength rev-max-shape)]
            (reify IndexingSystem$Forward
              (globalToLocal [item arg]
                (loop [idx (long 0)
                       arg (long arg)
                       offset (long 0)]
                  (if (and (> arg 0)
                           (< idx num-items))
                    (let [next-max (aget rev-max-shape idx)
                          next-stride (aget rev-strides idx)
                          next-dim (aget rev-shape idx)
                          max-idx (rem arg next-max)
                          shape-idx (rem arg next-dim)]
                      (recur (inc idx)
                             (quot arg next-max)
                             (+ offset (* next-stride shape-idx))))
                    offset)))))
          ;;Totally general case to encapsulate all the variations including indexed
          ;;dimensions.
          (let [reverse-shape (mapv (fn [item]
                                      (if (number? item)
                                        (long item)
                                        (dtype/->reader-of-type item :int32)))
                                    reverse-shape)
                reverse-stride (typecast/datatype->reader :int32
                                                          (int-array reverse-strides))
                reverse-max-shape (typecast/datatype->reader :int32
                                                             (int-array rev-max-shape))]
            (reify IndexingSystem$Forward
              (globalToLocal [item idx]
                (dims/elem-idx->addr reverse-shape reverse-strides
                                     reverse-max-shape idx)))))))))


(defn- naive-shape->strides
  "With no error checking, setup a new stride for the given shape.
  If some of the original strides are known, they can be passed in."
  [shape & [original-strides]]
  (let [shape (typecast/datatype->reader :int32 shape)
        n-shape (.size shape)
        n-original-strides (count original-strides)
        strides (int-array n-shape)]
    (loop [idx 0]
      (when (< idx n-shape)
        (let [local-idx (- n-shape idx 1)]
          (if (< idx n-original-strides)
            (aset strides local-idx (int (get original-strides
                                              (- n-original-strides idx 1))))
            (if (= idx 0)
              (aset strides local-idx 1)
              (aset strides local-idx (* (aget strides (+ local-idx 1))
                                         (.read shape (+ local-idx 1)))))))
        (recur (unchecked-inc idx))))
    strides))

(defn local-address->local-shape
  "Shape and strides are not transposed.  Returns
  [valid? local-shape-as-list]"
  [shape strides addr]
  (let [strides (typecast/datatype->reader :int32 strides)
        shape (typecast/datatype->reader :int32 shape)
        addr (int addr)
        n-elems (.size strides)
        retval (dtype/make-container :list :int32 0)
        retval-mut (typecast/datatype->mutable :int32 retval)]
    (loop [idx 0
           addr addr]
      (if (< idx n-elems)
        (let [local-stride (.read strides idx)
              shape-idx (quot addr local-stride)]
          (if (< shape-idx (.read shape idx))
            (do
              (.append retval-mut shape-idx)
              (recur (unchecked-inc idx) (rem addr local-stride)))
            (recur n-elems addr)))
        [(= 0 addr) retval]))))


(def integer-reduce-+ (-> (dtype-proto/->binary-op
                           (:+ binary-op/builtin-binary-ops)
                           :int32 true)
                          (dtype-proto/->reduce-op
                           :int32
                           true)))


(def integer-binary-* (dtype-proto/->binary-op
                       (:* binary-op/builtin-binary-ops)
                       :int32 true))


(defn dense-integer-dot-product
  "hardcode dot product for dense integers."
  [lhs rhs]
  (->> (binary-op/default-binary-reader-map
        {:datatype :int32} integer-binary-*
        lhs rhs)
       (reduce-op/default-iterable-reduce
        {:datatype :int32} integer-reduce-+)))


(defn get-elem-dims-local->global
  "Harder translation than above.  May return nil in the case where the inverse
  operation hasn't yet been derived.  In this case, the best you can do is a O(N)
  iteration similar to dense math."
  ^IndexingSystem$Backward
  [{:keys [shape strides] :as dims} max-shape]
  (let [dims-direct? (dims/direct? dims)
        access-increasing? (dims/access-increasing? dims)
        broadcasting? (not= shape max-shape)
        n-shape (count shape)
        [ordered-shape ordered-strides transpose-vec]
        (if access-increasing?
          [shape strides (reader/reader-range :int32 0 n-shape)]
          (let [index-ary (argsort/argsort strides {:datatype :int32 :reverse? true})]
            [(reader/make-indexed-reader index-ary shape {:datatype :object})
             (reader/make-indexed-reader index-ary strides {:datatype :int32})
             ;;In order to invert an arbitrary transposition, argsort it
             (argsort/argsort index-ary {:datatype :int32})]))
        shape-obj-reader (typecast/datatype->reader :object shape)
        safe-shape (shape/shape->count-vec shape)
        shape-int-reader (typecast/datatype->reader :int32 safe-shape)
        ordered-shape (typecast/datatype->reader :int32 (shape/shape->count-vec
                                                         ordered-shape))
        ordered-strides (typecast/datatype->reader :int32 ordered-strides)
        global-shape (typecast/datatype->reader :int32 (if (and access-increasing?
                                                                (= shape max-shape))
                                                         ordered-shape
                                                         (int-array max-shape)))
        global-strides (typecast/datatype->reader :int32  (naive-shape->strides
                                                           global-shape))
        shape-mults (typecast/datatype->reader :int32 (mapv quot max-shape safe-shape))
        n-global-shape (.size global-shape)]
    (reify
      IndexingSystem$Backward
      (localToGlobal [item local-idx]
        (let [[valid? local-shape] (local-address->local-shape
                                    ordered-shape ordered-strides local-idx)
              ;;move the local shape into the global space
              local-shape (typecast/datatype->reader
                           :int32
                           (reader/make-indexed-reader
                            transpose-vec local-shape {:datatype :int32
                                                       :unchecked? true}))
              local-shape
              (if dims-direct?
                local-shape
                (let [local-shape
                      (->> (map (fn [local-idx shape-entry]
                                  (cond
                                    (number? shape-entry)
                                    local-idx
                                    (shape/classified-sequence? shape-entry)
                                    (shape/classified-sequence->global-addr
                                     shape-entry local-idx)
                                    :else
                                    (when-let [addr (boolean-op/argfind
                                                     {:datatype :int32}
                                                     (boolean-op/make-boolean-unary-op
                                                      :int32
                                                      (= arg local-idx))
                                                     shape-entry)]
                                      addr)))
                                local-shape shape-obj-reader)
                           (remove nil?)
                           vec)]
                  (when (= (count local-shape)
                           n-global-shape)
                    local-shape)))]
          (when (and local-shape valid?)
            (let [local-shape (typecast/datatype->reader :int32
                                                         local-shape
                                                         true)
                  global-base-addr (int (dense-integer-dot-product
                                         local-shape global-strides))]
              (if-not broadcasting?
                [global-base-addr]
                ;;Expansion out into global space
                (let [retval (dtype/make-container :list :int32 [global-base-addr])]
                  (loop [idx (int 0)]
                    (when (< idx n-global-shape)
                      (let [local-idx (- n-global-shape idx 1)
                            num-repeat (- (.read shape-mults local-idx) 1)
                            multiplier (* (.read global-strides local-idx)
                                          (.read shape-int-reader local-idx))
                            ;;This is not something that would work in c++.  The unary
                            ;;operation will make a reader out of retval which gets the
                            ;;underyling nio buffer of fixed size.  Then the list will
                            ;;perform insertions at the end evaulating the unary map
                            ;;exactly over this initial reader.
                            initial-reader (typecast/datatype->reader
                                            :int32 retval true)]
                        (loop [repeat-idx 0]
                          (when (< repeat-idx num-repeat)
                            (let [local-mult (* multiplier (+ repeat-idx 1))]
                              (dtype/insert-block! retval (dtype/ecount retval)
                                                   (unary-op/default-unary-reader-map
                                                    {:datatype :int32 :unchecked? true}
                                                    (unary-op/make-unary-op
                                                     :int32 (+ arg local-mult))
                                                    initial-reader)))
                            (recur (unchecked-inc repeat-idx)))))
                      (recur (unchecked-inc idx))))
                  retval)))))))))
