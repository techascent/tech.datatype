(ns tech.tensor.index-system
  (:require [tech.datatype :as dtype]
            [tech.datatype.base :as dtype-base]
            [tech.tensor.dimensions :as dims])
  (:import [tech.datatype
            IndexingSystem$Forward
            IndexingSystem$Backward]))

(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn get-elem-dims->address
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
            (dims/->reverse-data dims max-shape)]
        (if direct?
          (->ElemIdxToAddr (int-array reverse-shape) (int-array reverse-strides)
                           (int-array (vec (reverse max-shape))))
          (do
            (->GeneralElemIdxToAddr (mapv (fn [item]
                                            (cond-> item
                                              (ct/tensor? item)
                                              ct/tensor->buffer))
                                          reverse-shape)
                                    reverse-strides
                                    (ct-utils/reversev max-shape))))))))
