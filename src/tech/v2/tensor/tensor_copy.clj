(ns tech.v2.tensor.tensor-copy
  (:require [tech.v2.tensor.impl :as tens-impl]
            [tech.v2.tensor.dimensions :as dims]
            [tech.v2.tensor.dimensions.global-to-local :as gtol]))


(defn setup-dims-bit-blit
  [src-dims dst-dims]
  (let [src-offsets? (gtol/any-offsets? src-dims)
        dst-offsets? (gtol/any-offsets? dst-dims)
        src-dims (gtol/reduce-dimensionality src-dims src-offsets? true)
        dst-dims (gtol/reduce-dimensionality dst-dims dst-offsets? true)
        ^objects src-shape (:shape src-dims)
        ^longs src-strides (:strides src-dims)
        n-src (alength src-shape)
        n-src-dec (dec n-src)
        ^objects dst-shape (:shape dst-dims)
        ^longs dst-strides (:strides dst-dims)
        n-dst (alength dst-shape)
        n-dst-dec (dec n-dst)

        both-direct? (and (number? (aget src-shape n-src-dec))
                          (number? (aget dst-shape n-dst-dec)))
        strides-packed? (and (== 1 (aget src-strides n-src-dec))
                             (== 1 (aget dst-strides n-dst-dec)))
        no-last-offsets? (or (not (or src-offsets? dst-offsets?)))]
    (when (and both-direct? strides-packed?)
      (let [dst-last-shape (long (aget dst-shape n-dst-dec))
            src-last-shape (long (aget src-shape n-src-dec))
            min-shape (min dst-last-shape src-last-shape)
            max-shape (max dst-last-shape src-last-shape)]
        ;;the shapes have to be commensurate
        (when (== 0 (rem max-shape min-shape))

          )



        [src-dims dst-dims]))))



(comment
  (def src-dims (dims/dimensions [256 256 4]))
  (def dst-dims (dims/dimensions [256 256 4]
                                 :strides [8192 4 1]))
  (def bb-test (setup-dims-bit-blit src-dims dst-dims))
  )
