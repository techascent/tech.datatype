(ns tech.tensor
  (:require [tech.datatype :as dtype]
            [tech.datatype.protocols :as dtype-proto]
            [tech.tensor.impl :as impl]
            [tech.tensor.dimensions :as dims]
            [tech.tensor.dimensions.shape :as shape]
            [tech.datatype.functional :as dtype-fn]
            [tech.datatype.functional.impl :as func-impl]))


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
                          ->core-matrix
                          ->core-matrix-vector
                          ->jvm)
