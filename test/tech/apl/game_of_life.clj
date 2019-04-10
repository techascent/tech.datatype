(ns tech.apl.game-of-life
  "https://youtu.be/a9xAKttWgP4"
  (:require [tech.tensor :as tens]
            [tech.datatype :as dtype]
            [tech.datatype.boolean-op :as bool-op]
            [tech.tensor.impl :as tens-impl]
            [tech.datatype.functional :as fn]))


(defn membership
  [lhs rhs]
  (let [membership-set (set (vec rhs))]
    (bool-op/boolean-unary-reader
     {:datatype :object}
     (bool-op/make-boolean-unary-op
      :object (contains? membership-set arg))
     lhs)))


(defn apl-take
  [item new-shape]
  (let [item-shape (dtype/shape item)
        min-shape (mapv min item-shape new-shape)
        reshape-item (apply tens/select item (map range min-shape))
        retval (tens/new-tensor new-shape :datatype (dtype/get-datatype item))
        copy-item (apply tens/select retval (map range min-shape))]
    (dtype/copy! (dtype/->reader reshape-item)
                 (dtype/->writer copy-item))
    retval))


(def range-tens (tens/reshape (vec (range 9)) [3 3]))


(def bool-tens (-> range-tens
                   (membership [1 2 3 4 7])))

(def take-tens (apl-take bool-tens [5 7]))
