(ns tech.tensor
  (:require [tech.datatype :as dtype]
            [tech.datatype.protocols :as dtype-proto]
            [tech.tensor.impl :as impl]
            [tech.tensor.dimensions :as dims]
            [tech.tensor.dimensions.shape :as shape]
            [tech.datatype.functional :as dtype-fn]
            [tech.datatype.functional.impl :as func-impl]
            [tech.datatype.sparse.protocols :as sparse-proto]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.binary-op :as binary-op]))



(func-impl/export-symbols tech.tensor.impl
                          ->tensor
                          new-tensor
                          clone
                          reshape
                          select
                          transpose
                          broadcast
                          rotate
                          tensor?
                          ensure-tensor
                          tensor-force
                          tensor-container-type
                          tensor-buffer-type
                          mutable?
                          matrix-matrix-dispatch
                          ->core-matrix
                          ->core-matrix-vector
                          ->jvm)


(func-impl/export-symbols tech.tensor.typecast
                          datatype->tensor-reader)


(defn dimensions-dense?
  [tensor]
  (dims/dense? (:dimensions tensor)))



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
