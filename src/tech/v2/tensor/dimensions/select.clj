(ns tech.v2.tensor.dimensions.select
  "Selecting subsets from a larger set of dimensions leads to its own algebra."
  (:require [tech.v2.tensor.dimensions.shape :as shape]
            [tech.v2.tensor.utils :refer [when-not-error]]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.monotonic-range :as dtype-range]
            [tech.v2.datatype.protocols :as dtype-proto])
  (:import [tech.v2.datatype LongReader]
           [java.util ArrayList List]
           [clojure.lang IMeta]
           [it.unimi.dsi.fastutil.longs LongArrayList]))

(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn- expand-dimension
  ^LongReader [dim]
  (shape/classify-sequence dim))


(defn- expand-select-arg
  [select-arg n-dim]
  (cond
    (= :all select-arg)
    (dtype-range/make-range (long n-dim))
    (= :lla select-arg)
    (dtype-range/reverse-range n-dim)
    :else
    (shape/classify-sequence select-arg)))


(defn apply-select-arg-to-dimension
  "Given a dimension and select argument, create a new dimension with
the selection applied."
  [dim select-arg]
  (if (= select-arg :all)
    dim
    (let [expanded-dim (expand-dimension dim)
          n-elems (.lsize ^LongReader dim)]
      (if (number? select-arg)
        (let [dim-val (.read expanded-dim (long select-arg))]
          (with-meta
            (dtype-range/make-range dim-val (unchecked-inc dim-val))
            {:scalar? true}))
        (shape/combine-classified-sequences
         expanded-dim
         (expand-select-arg select-arg n-elems))))))


(defn select
  "Apply a select seq to a dimension.
  Return new shape, stride, offset array
  along with new buffer offset and if it can be calculated a new
  buffer length."
  [select-seq shape-vec stride-vec offset-vec]
  (let [^List select-vec (vec select-seq)
        ^List shape-vec shape-vec
        ^List stride-vec stride-vec
        ^List offset-vec offset-vec
        result-shape (ArrayList.)
        result-stride (LongArrayList.)
        result-offset (LongArrayList.)
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
        (let [^LongReader new-shape-val (apply-select-arg-to-dimension
                                         (.get shape-vec idx)
                                         (.get select-vec idx))
              ;;scalar means to evaluate the result immediately and do not add to result dims.
              new-shape-scalar? (:scalar? (meta new-shape-val))
              stride-val (long (.get stride-vec idx))
              offset-val (long (.get offset-vec idx))
              constant-min-max? (dtype-proto/has-constant-time-min-max? new-shape-val)
              length-valid? (boolean (and length-valid? constant-min-max?))
              range? (dtype-proto/convertible-to-range? new-shapeval)
              buffer-offset (long (if constant-min-max?
                                    (+ buffer-offset
                                       (* stride-val
                                          (let [(long (dtype-proto/constant-time-min new-shape-val))]
                                            (if (== 0 offset-val)
                                              min-val
                                              (long (rem (+ offset-val min-val)
                                                         (dtype/ecount new-shape-val)))))))
                                    buffer-offset))
              max-stride (max max-stride stride-val)
              max-shape (long (if (= max-stride stride-val)
                                (if (== 0 offset-val)
                                  (unchecked-inc (long (dtype-proto/constant-time-max new-shapeval))))
                                max-shape))]
          (when-not (:scalar? (meta new-shape-val))



            )
          (recur (unchecked-inc idx offset length-valid max-stride max-shape-val)))
        {:shape result-shape
         :strides result-stride
         :offsets result-offset
         :offset offset
         :length (when length-valid?
                   (* max-stride max-shape-val))}))))


#_(defn dimensions->simpified-dimensions
  "Given the dimensions post selection, produce a new dimension sequence combined with
  an offset that lets us know how much we should offset the base storage type.
  Simplification is important because it allows a backend to hit more fast paths.
Returns:
{:dimension-seq dimension-seq
:offset offset}"
  [dimension-seq stride-seq dim-offset-seq]
  (let [[dimension-seq strides dim-offsets offset]
        (reduce
         (fn [[dimension-seq strides dim-offsets offset]
              [dimension stride dim-offset]]
           (let [dim-type (if (map? dimension)
                            :classified-seqence
                            :reader)
                 [dim-type dimension]
                 (if (and (= :reader dim-type)
                          (= 1 (dtype/ecount dimension)))
                   (let [dim-val (dtype/get-value dimension 0)]
                     [:classified-sequence
                      {:type :+ :min-item dim-val :max-item dim-val}])
                   [dim-type dimension])]
             (case dim-type
               :reader
               [(conj dimension-seq dimension)
                (conj strides stride)
                (conj dim-offsets dim-offset)
                offset]
               :classified-sequence
               ;;Shift the sequence down and record the new offset.
               (let [{:keys [type min-item max-item sequence
                             scalar?]} dimension
                     max-item (- (long max-item) (long min-item))
                     new-offset (+ (long offset)
                                   (* (long stride)
                                      (long (:min-item dimension))))
                     min-item 0
                     dimension (cond-> (assoc dimension
                                              :min-item min-item
                                              :max-item max-item)
                                 sequence
                                 (assoc :sequence (mapv (fn [idx]
                                                          (- (long idx)
                                                             (long min-item)))
                                                        sequence)))
                     ;;Now simplify the dimension if possible
                     dimension (cond
                                 (= min-item max-item)
                                 1
                                 (= :+ type)
                                 (+ 1 max-item)
                                 sequence
                                 (:sequence dimension)
                                 :else
                                 dimension)]
                 ;;A scalar single select arg means drop the dimension.
                 (if-not (and (= 1 dimension)
                              scalar?
                              (= 0 (int dim-offset)))
                   [(conj dimension-seq dimension)
                    (conj strides stride)
                    (conj dim-offsets dim-offset)
                    new-offset]
                   ;;We keep track of offsetting but we don't add the
                   ;;element to the return value.
                   [dimension-seq strides dim-offsets new-offset]))
               ;;Only readers and classified sequences allowed here.
               (throw (ex-info "Bad dimension type"
                               {:dimension dimension})))))
         [[] [] [] 0]
         (map vector dimension-seq stride-seq dim-offset-seq))
        retval

        {:dimension-seq dimension-seq
         :strides strides
         :offsets dim-offsets
         :offset offset
         :length (when (shape/direct-shape? dimension-seq)
                   (apply + 1 (map * (map (comp dec shape/shape-entry->count)
                                          dimension-seq) strides)))}]
    retval))
