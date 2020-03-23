(ns tech.v2.tensor.tensor-copy
  (:require [tech.v2.tensor.impl :as tens-impl]
            [tech.v2.tensor.dimensions :as dims]
            [tech.v2.datatype.index-algebra :as idx-alg]
            [tech.v2.tensor.dimensions.global-to-local :as gtol]
            [tech.v2.tensor.dimensions.analytics :as dims-analytics]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.fast-copy :as fast-copy]
            [tech.parallel.for :refer [parallel-for serial-for]])
  (:import [tech.v2.datatype LongReader]
           [java.util List]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn setup-dims-bit-blit
  [src-dims dst-dims]
  (let [src-offsets? (dims-analytics/any-offsets? src-dims)
        dst-offsets? (dims-analytics/any-offsets? dst-dims)
        src-dims (dims-analytics/reduce-dimensionality src-dims src-offsets?)
        dst-dims (dims-analytics/reduce-dimensionality dst-dims dst-offsets?)
        ^List src-shape (:shape src-dims)
        ^List src-strides (:strides src-dims)
        ^List src-max-shape (:shape-ecounts src-dims)
        n-src (.size src-shape)
        n-src-dec (dec n-src)
        ^List dst-shape (:shape dst-dims)
        ^List dst-strides (:strides dst-dims)
        ^List dst-max-shape (:shape-ecounts dst-dims)
        n-dst (.size dst-shape)
        n-dst-dec (dec n-dst)

        both-direct? (and (number? (.get src-shape n-src-dec))
                          (number? (.get dst-shape n-dst-dec)))
        strides-packed? (and (== 1 (long (.get src-strides n-src-dec)))
                             (== 1 (long (.get dst-strides n-dst-dec))))
        src-last-offset (long (if src-offsets?
                                (idx-alg/get-offset
                                 (.get src-shape n-src-dec))
                                0))
        dst-last-offset (long
                         (if dst-offsets?
                           (idx-alg/get-offset
                            (.get dst-shape n-dst-dec))
                           0))]
    (when (and both-direct?
               strides-packed?
               (== 0 src-last-offset)
               (== 0 dst-last-offset))
      (let [dst-last-shape (long (.get dst-shape n-dst-dec))
            src-last-shape (long (.get src-shape n-src-dec))
            min-shape (min dst-last-shape src-last-shape)
            max-shape (max dst-last-shape src-last-shape)]
        ;;the shapes have to be commensurate
        (when (and (== 0 (rem max-shape min-shape))
                   (== dst-last-shape (long (.get dst-max-shape n-dst-dec)))
                   (== src-last-shape (long (.get src-max-shape n-src-dec))))
          (let [src-reader (gtol/reduced-dims->global->local-reader src-dims)
                dst-reader (gtol/reduced-dims->global->local-reader dst-dims)]
            {:block-size min-shape
             :n-blocks (quot (.lsize src-reader)
                             min-shape)
             :src-offset-reader src-reader
             :dst-offset-reader dst-reader}))))))


(defn bit-blit!
  "Returns :ok if bit blit succeeds"
  ([dst src options]
   (let [unchecked? (:unchecked? options)
         src-dtype (dtype/get-datatype src)
         dst-dtype (dtype/get-datatype dst)
         ;;Getting the buffers defeats a check in the tensors
         ;;to avoid giving you those if
         src-buffer (tens-impl/tensor->buffer src)
         dst-buffer (tens-impl/tensor->buffer dst)
         src-buf-type (dtype/buffer-type src-buffer)
         dst-buf-type (dtype/buffer-type dst-buffer)
         src-nio (dtype/as-nio-buffer src-buffer)
         dst-nio (dtype/as-nio-buffer dst-buffer)]
     (when (and (= :dense src-buf-type)
                (= :dense dst-buf-type)
                src-nio
                dst-nio
                (or unchecked?
                    (= src-dtype
                       dst-dtype)))
       (let [src-nio-dtype (dtype/get-datatype src-nio)
             dst-nio-dtype (dtype/get-datatype dst-nio)]
         (when (= src-nio-dtype dst-nio-dtype)
           (when-let [dims-data (setup-dims-bit-blit
                                 (tens-impl/tensor->dimensions src)
                                 (tens-impl/tensor->dimensions dst))]
             (let [block-size (long (:block-size dims-data))
                   n-blocks (long (:n-blocks dims-data))
                   ^LongReader src-offset-reader (:src-offset-reader dims-data)
                   ^LongReader dst-offset-reader (:dst-offset-reader dims-data)]
               (when (>= block-size 512)
                 (do
                   (parallel-for
                    idx
                    n-blocks
                    (let [offset (* idx block-size)
                          src-offset (.read src-offset-reader offset)
                          dst-offset (.read dst-offset-reader offset)]
                      (fast-copy/copy! (dtype-proto/sub-buffer dst-nio dst-offset
                                                               block-size)
                                       (dtype-proto/sub-buffer src-nio src-offset
                                                               block-size))))
                   :ok)))))))))
  ([dst src]
   (bit-blit! dst src {})))


(defmethod dtype-proto/copy! [:tensor :tensor]
  [dst src options]
  (when-not (= (dtype/shape dst)
               (dtype/shape src))
    (throw (Exception. (format "src shape %s doesn't match dst shape %s"
                               (dtype/shape src)
                               (dtype/shape dst)))))
  (when-not (= :ok (bit-blit! dst src options))
    (dtype-proto/copy! (tens-impl/tensor->base-buffer-type dst)
                       (tens-impl/tensor->base-buffer-type src)
                       options))
  dst)



(comment
  (def src-dims (dims/dimensions [256 256 4]))
  (def dst-dims (dims/dimensions [256 256 4]
                                 :strides [8192 4 1]))

  (def src-tens (tens-impl/new-tensor [256 256 4] :datatype :uint8))
  (dtype/copy! (range (* 256 256 4)) 0 src-tens 0 (* 256 256 4) {:unchecked? true})
  (def dst-img-tens (tens-impl/new-tensor [2048 2048 4] :datatype :uint8))
  (def dst-tens (tens-impl/select dst-img-tens (range 256) (range 256) :all))
  (def bit-blit-result (bit-blit! dst-tens src-tens))
  )
