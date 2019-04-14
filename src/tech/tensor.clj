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
            [tech.datatype.binary-op :as binary-op]
            [tech.libs.blas :as blas]))


(defn ->tensor
  [data & {:keys [datatype container-type]
           :as options}]
  (let [data-shape (dtype/shape data)
        datatype (impl/datatype datatype)
        container-type (impl/container-type container-type)
        n-elems (apply * 1 data-shape)]
    (impl/construct-tensor
     (first
      (dtype/copy-raw->item!
       data
       (dtype/make-container container-type datatype n-elems options)
       0 options))
     (dims/dimensions data-shape))))


(defn new-tensor
  [shape & {:keys [datatype container-type]
            :as options}]
  (let [datatype (impl/datatype datatype)
        container-type (impl/container-type container-type)
        n-elems (apply * 1 shape)]
    (impl/construct-tensor
     (dtype/make-container container-type datatype n-elems options)
     (dims/dimensions shape))))


(defn clone
  [tens & {:keys [datatype
                  container-type]}]
  (let [datatype (or datatype (dtype/get-datatype tens))
        container-type (impl/container-type (or container-type
                                                (dtype/container-type tens)))
        new-buffer (if (satisfies? dtype-proto/PPrototype (impl/tensor->buffer tens))
                     (dtype/from-prototype (impl/tensor->buffer tens)
                                           :datatype datatype
                                           :shape (dtype/shape
                                                   (impl/tensor->buffer tens)))
                     (dtype/make-container (impl/container-type container-type)
                                           datatype
                                           (dtype/ecount tens)))
        new-tens (impl/construct-tensor
                  new-buffer
                  (dims/dimensions (dtype/shape tens)))]
    (dtype/copy! tens new-tens)))


(defn tensor-force
  "Ensure any delayed operations happen for this and reads from this tensor
  happen reasonably fast.  For sparse this probably means cloning."
  [tens]
  (let [buffer-type (dtype/buffer-type (:buffer tens))
        new-tens (if (= :sparse buffer-type)
                   (impl/construct-tensor
                    (sparse-proto/->sparse-reader tens)
                    (dims/dimensions (dtype/shape tens)))
                   ;;force a potentially deep reader chain.
                   (if (or (not (impl/simple-dimensions? (:dimensions tens)))
                           ;;In the case of a reader chain, we will no longer
                           ;;be able to get the buffer back from the tensor.
                           (not (dtype-proto/as-nio-buffer tens)))
                     (clone tens)
                     tens))]
    ;;force actual creation of dimension transforms
    (dims/->global->local (:dimensions new-tens))
    ;;Sparse always needs the inverse transform
    (when (= :sparse buffer-type)
      (dims/->local->global (:dimensions new-tens)))
    new-tens))



(defn rotate
  [tens rotate-vec]
  (let [tens (impl/ensure-tensor tens)]
    (assoc tens :dimensions
           (dims/rotate (impl/tensor->dimensions tens)
                        (mapv #(* -1 (long %)) rotate-vec)))))


(defn reshape
  [tens new-shape]
  (let [tens (impl/ensure-tensor tens)
        new-dims (dims/in-place-reshape (:dimensions tens)
                                        new-shape)]
    (impl/construct-tensor
     (dtype/sub-buffer (impl/tensor->buffer tens)
                       0 (dims/buffer-ecount new-dims))
     new-dims)))


(defn transpose
  [tens transpose-vec]
  (let [tens (impl/ensure-tensor tens)]
    (update tens :dimensions dims/transpose transpose-vec)))


(defn select
  [tens & args]
  (let [tens (impl/ensure-tensor tens)
        {new-dims :dims
         buf-offset :elem-offset
         buf-len :buffer-length}
        (apply dims/select (:dimensions tens) args)]
    (impl/construct-tensor (-> (impl/tensor->buffer tens)
                               (dtype/sub-buffer buf-offset buf-len))
                           new-dims)))


(defn broadcast
  "Create a larger tensor by repeating dimensions of a smaller tensor."
  [tens bcast-shape]
  (let [tens-shape (dtype/shape tens)
        n-tens-elems (dtype/ecount tens)
        n-bcast-elems (shape/ecount bcast-shape)
        num-tens-shape (count tens-shape)
        {:keys [shape strides offsets max-shape]
         :as tens-dims} (:dimensions tens)]
    (when-not (every? number? bcast-shape)
      (throw (ex-info "Broadcast shapes must only be numbers" {})))
    (when-not (>= n-bcast-elems
                  n-tens-elems)
      (throw (ex-info
              (format "Improper broadcast shape (%s), smaller than tens (%s)"
                              bcast-shape tens-shape)
                      {})))
    (when-not (every? (fn [[item-dim bcast-dim]]
                        (= 0 (rem (int bcast-dim)
                                  (int item-dim))))
                      (map vector tens-shape (take-last num-tens-shape bcast-shape)))
      (throw (ex-info
              (format "Broadcast shape (%s) is not commensurate with tensor shape %s"
                              bcast-shape tens-shape)
                      {})))
    (assoc tens :dimensions
           (dims/dimensions shape :strides strides :offsets offsets
                            :max-shape bcast-shape))))



(func-impl/export-symbols tech.tensor.impl
                          tensor?
                          ensure-tensor
                          mutable?
                          ->core-matrix
                          ->core-matrix-vector
                          ->jvm)


(defn tensor-buffer-type
  [tens]
  (if (tensor? tens)
    (dtype/buffer-type (:buffer tens))
    (dtype/buffer-type tens)))


(defn tensor-container-type
  [tens]
  (if (tensor? tens)
    (dtype/container-type (:buffer tens))
    (dtype/container-type tens)))


(defmulti matrix-matrix-dispatch
  (fn [alpha lhs rhs bin-op reduce-op options]
    [(tensor-buffer-type lhs)
     (tensor-buffer-type rhs)
     (dtype-base/op-name bin-op)
     (dtype-base/op-name reduce-op)]))

(defn- mmul-check
  [lhs rhs]
  (let [lhs-shape (dtype/shape lhs)
        rhs-shape (dtype/shape rhs)
        rhs-shape (if (= 1 (count rhs-shape))
                    [(first rhs-shape) 1]
                    rhs-shape)]
    (when-not (and (= 2 (count lhs-shape))
                   (= 2 (count rhs-shape)))
      (throw (ex-info "Both items must have shape count 2" {})))
    (when-not (= (second lhs-shape) (first rhs-shape))
      (throw (ex-info "Inner dimensions don't match"
                      {:lhs-shape lhs-shape
                       :rhs-shape rhs-shape})))
    [lhs-shape rhs-shape]))


(defn- default-matrix-matrix
  [alpha lhs rhs bin-op reduce-op options]
  (let [[lhs-shape rhs-shape] (mmul-check lhs rhs)
        lhs-rows (->> (range (first lhs-shape))
                      (mapv #(select lhs % :all)))
        rhs-columns (->> (range (second rhs-shape))
                         (mapv #(select rhs :all %)))
        result-container-type (or (:container-type options)
                                  (if (and (= :sparse (dtype/container-type lhs))
                                           (= :sparse (dtype/container-type rhs)))
                                    :sparse
                                    :list))
        datatype (or (:datatype options)
                     (dtype/get-datatype lhs))
        new-tens (impl/construct-tensor
                  (->> (for [row lhs-rows
                             col rhs-columns]
                         [row col])
                       (pmap #(dtype-fn/dot-product (first %)
                                                    (second %)
                                                    bin-op reduce-op
                                                    {:datatype datatype}))
                       (dtype/make-container result-container-type datatype))
                  (dims/dimensions [(first lhs-shape) (second rhs-shape)]))]
    (cond->> new-tens
      alpha
      (dtype-fn/apply-binary-op {:datatype datatype} bin-op alpha))))


(defmethod matrix-matrix-dispatch :default
  [alpha lhs rhs bin-op reduce-op options]
  (default-matrix-matrix alpha lhs rhs bin-op reduce-op options))


(defn- external-force-dense
  "Extern fn calls can take any stride order but they cannot take
  offsets or a reader chain."
  [tens]
  (let [any-offsets? (every? #(= 0 %) (:offsets (:dimensions tens)))
        nio-access? (dtype-proto/as-nio-buffer tens)]
    (if (and (not any-offsets?)
             nio-access?)
      (clone tens)
      tens)))


(defmethod matrix-matrix-dispatch [:dense :dense :* :+]
  [alpha lhs rhs bin-op reduce-op options]
  (let [lhs-dtype (dtype/get-datatype lhs)]
    (if (and (or (= lhs-dtype :float32)
                 (= lhs-dtype :float64))
             (blas/has-blas?))
      (let [[lhs-shape rhs-shape] (mmul-check lhs rhs)
            lhs (external-force-dense lhs)
            rhs (external-force-dense rhs)
            alpha (if alpha (double alpha) 1.0)
            beta 0.0
            C (new-tensor [(first lhs-shape) (second rhs-shape)]
                          :datatype lhs-dtype
                          :container-type :typed-buffer)
            lhs-strides (get-in lhs [:dimensions :strides])
            rhs-strides (get-in rhs [:dimensions :strides])
            lhs-min-stride (int (apply min lhs-strides))
            rhs-min-stride (int (apply min rhs-strides))
            lhs-trans? (= lhs-min-stride (first lhs-strides))
            rhs-trans? (= rhs-min-stride (first rhs-strides))
            gemv? (= 1 (second rhs-shape))]
        (println gemv? lhs-trans? rhs-trans? lhs-shape rhs-shape
                 lhs-min-stride rhs-min-stride
                 lhs-strides rhs-strides)
        (if gemv?
          ((case lhs-dtype
             :float32 blas/cblas_sgemv
             :float64 blas/cblas_dgemv)
           :row-major lhs-trans?
           (first lhs-shape)  (second lhs-shape)
           alpha (:buffer lhs) lhs-min-stride (:buffer rhs) rhs-min-stride
           beta (:buffer C) 1)
          (do
            (println "gemm case" alpha beta
                     (dtype/->vector (:buffer lhs))
                     (dtype/->vector (:buffer rhs))
                     (dtype/->vector (:buffer C)))
            (blas/cblas_dgemm
             :column-major false false
             (first lhs-shape) (first rhs-shape) (second lhs-shape)
             alpha (:buffer lhs) 1 (:buffer rhs) 1
             beta (:buffer C) 1)
            (println C)
            (comment ((case lhs-dtype
                        :float32 blas/cblas_sgemm
                        :float64 blas/cblas_dgemm)
                      :row-major lhs-trans? rhs-trans?
                      (first lhs-shape) (second lhs-shape) (second rhs-shape)
                      alpha (:buffer lhs) lhs-min-stride (:buffer rhs) rhs-min-stride
                      beta (:buffer C) 1))))
        C)
      (default-matrix-matrix alpha lhs rhs bin-op reduce-op options))))


(defn matrix-multiply
  "lhs - 2 dimensional tensor.
  rhs - Either 2 dimensional tensor or 1 dimensional vector.
  reduction operators - *,+"
  [lhs rhs]
  (matrix-matrix-dispatch nil lhs rhs
                          (:* binary-op/builtin-binary-ops)
                          (:+ binary-op/builtin-binary-ops)
                          {}))
