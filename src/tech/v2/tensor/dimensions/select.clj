(ns tech.v2.tensor.dimensions.select
  "Selecting subsets from a larger set of dimensions leads to its own algebra."
  (:require [tech.v2.datatype.index-algebra :as idx-alg]
            [tech.v2.datatype.protocols :as dtype-proto])
  (:import [tech.v2.datatype LongReader]
           [java.util ArrayList List]
           [it.unimi.dsi.fastutil.longs LongArrayList]))

(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn- expand-dimension
  ^LongReader [dim]
  (idx-alg/dimension->reader dim))


(defn apply-select-arg-to-dimension
  "Given a dimension and select argument, create a new dimension with
the selection applied."
  [dim select-arg]
  (if (= select-arg :all)
    dim
    (idx-alg/select dim select-arg)))


(defn select
  "Apply a select seq to a dimension.
  Return new shape, stride, offset array
  along with new buffer offset and if it can be calculated a new
  buffer length."
  [select-seq shape-vec stride-vec offset-vec]
  (let [^List select-vec (vec select-seq)
        ^List shape-vec shape-vec
        ^List stride-vec stride-vec
        result-shape (ArrayList.)
        result-stride (LongArrayList.)
        n-elems (.size select-vec)]
    (when-not (== (.size select-vec)
                  (.size shape-vec))
      (throw (Exception. "Shape,select vecs do not match")))
    (loop [idx 0
           buffer-offset 0
           length-valid? true
           max-stride 0
           max-shape-val 0]
      (if (< idx n-elems)
        (let [new-shape-val (apply-select-arg-to-dimension
                             (.get shape-vec idx)
                             (.get select-vec idx))
              ;;scalar means to evaluate the result immediately and do not
              ;;add to result dims.
              new-shape-scalar? (:select-scalar? (meta new-shape-val))
              stride-val (long (.get stride-vec idx))
              constant-min-max? (dtype-proto/has-constant-time-min-max? new-shape-val)
              length-valid? (boolean (and length-valid? constant-min-max?))
              buffer-offset (long (if constant-min-max?
                                    (+ buffer-offset
                                       (* stride-val
                                          (long (dtype-proto/constant-time-min
                                                 new-shape-val))))
                                    buffer-offset))
              max-stride (max max-stride stride-val)
              max-shape-val
              (long (if (== max-stride stride-val)
                      (unchecked-inc (long (dtype-proto/constant-time-max
                                            new-shape-val)))
                      max-shape-val))]
          (when-not new-shape-scalar?
            (.add result-shape new-shape-val)
            (.add result-stride stride-val))
          (recur (unchecked-inc idx) buffer-offset
                 length-valid? max-stride max-shape-val))
        {:shape result-shape
         :strides result-stride
         :offset buffer-offset
         :length (when length-valid?
                   (* max-stride max-shape-val))}))))
