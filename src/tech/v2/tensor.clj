(ns tech.v2.tensor
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.tensor.impl :as impl]
            [tech.v2.tensor.dimensions :as dims]
            [tech.v2.tensor.protocols :as tens-proto]
            ;;Ensure printing things works.
            [tech.v2.tensor.pprint]
            [tech.v2.datatype.functional.impl :as func-impl]
            [tech.v2.datatype.binary-op :as binary-op]))


(func-impl/export-symbols tech.v2.tensor.impl
                          ->tensor
                          new-tensor
                          clone
                          reshape
                          select
                          transpose
                          broadcast
                          rotate
                          slice
                          tensor?
                          ensure-tensor
                          ensure-buffer-descriptor
                          buffer-descriptor->tensor
                          tensor-force
                          tensor-container-type
                          tensor-buffer-type
                          mutable?
                          matrix-matrix-dispatch
                          ->jvm)


(defn as-buffer-descriptor
  "Convenience function.  Also present in tech.v2.datatype, just here because
  ensure-buffer-descriptor is in this namespace."
  [tens]
  (dtype/as-buffer-descriptor tens))


(defn tensor->dimensions
  [tens]
  (tens-proto/dimensions tens))


(defn tensor->buffer
  [tens]
  (tens-proto/buffer tens))


(defn dimensions-dense?
  [tensor]
  (dims/dense? (tensor->dimensions tensor)))


(defn rows
  [tens]
  (let [[n-rows _] (dtype/shape tens)]
    (->> (range n-rows)
         (map #(select tens % :all)))))


(defn columns
  [tens]
  (let [[_ n-cols] (dtype/shape tens)]
    (->> (range n-cols)
         (map #(select tens :all %)))))



(defn matrix-multiply
  "lhs - 2 dimensional tensor.
  rhs - Either 2 dimensional tensor or 1 dimensional vector.
  alpha - multiply result by alpha.
  reduction operators - *,+"
  [lhs rhs & [alpha]]
  (when-not (= (dtype/get-datatype lhs)
               (dtype/get-datatype rhs))
    (throw (ex-info (format "Argument datatype mismatch: %s vs %s"
                            (name (dtype/get-datatype lhs))
                            (name (dtype/get-datatype rhs)))
                    {})))
  (impl/matrix-matrix-dispatch alpha lhs rhs
                               (:* binary-op/builtin-binary-ops)
                               (:+ binary-op/builtin-binary-ops)
                               {}))
