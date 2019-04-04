(ns tech.compute.tensor.dimensions.select
  "Selecting subsets from a larger set of dimensions leads to its own algebra."
  (:require [tech.compute.tensor.protocols :refer [tensor? dense?]]
            [tech.compute.tensor.dimensions.shape :as shape]
            [tech.compute.tensor.utils :refer [when-not-error]]
            [clojure.core.matrix :as m]
            [tech.datatype :as dtype]))

(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn- expand-dimension
  [dim]
  (cond (number? dim)
        {:type :+
         :min-item 0
         :max-item (- (long dim) 1)}
        (tensor? dim) dim
        (shape/classified-sequence? dim) dim
        (sequential? dim) (shape/classify-sequence dim)
        :else
        (throw (ex-info "Failed to recognize dimension type"
                        {:dimension dim}))))


(defn- verify-tensor-indexer
  [select-arg]
  (let [tens-shape (dtype/shape select-arg)]
    (when-not (= 1 (count tens-shape))
      (throw (ex-info "Only tensor of rank 1 can be used as indexes in other tensors"
                      {:select-arg-shape tens-shape})))
    (when-not (dense? select-arg)
      (throw (ex-info "Only dense tensors can be used as indexes in other tensors" {})))
    (when-not (= :int32 (dtype/get-datatype select-arg))
      (throw (ex-info "Index tensors must be int32 datatype"
                      {:tensor-datatype (dtype/get-datatype select-arg)})))
    select-arg))


(defn- expand-select-arg
  [select-arg]
  (cond
    (number? select-arg) (shape/classify-sequence select-arg)
    (tensor? select-arg) (verify-tensor-indexer select-arg)
    (shape/classified-sequence? select-arg) select-arg
    (sequential? select-arg) (shape/classify-sequence select-arg)
    (= :all select-arg) select-arg
    (= :lla select-arg) select-arg
    :else
    (throw (ex-info "Unrecognized select argument type"
                    {:select-arg select-arg}))))


(defn apply-select-arg-to-dimension
  "Given a dimension and select argument, create a new dimension with
the selection applied."
  [dim select-arg]
  ;;Dim is now a map or a tensor
  (let [dim (expand-dimension dim)
        ;;Select arg is now a map, a keyword, or a tensor
        select-arg (expand-select-arg select-arg)
        dim-type (cond (tensor? dim) :tensor
                       (shape/classified-sequence? dim)  :classified-sequence)
        select-type (cond (tensor? select-arg) :tensor
                          (shape/classified-sequence? select-arg) :classified-sequence
                          (keyword? select-arg) :keyword)]
    (cond
      (= :tensor select-type)
      (do
        (when-not-error (and (= :classified-sequence dim-type)
                             (= :+ (:type dim))
                             (= 0 (long (:min-item dim))))
          "Can only use tensor indexers on monotonically incrementing dimensions"
          {:dim dim
           :select-arg select-arg})
        select-arg)
      (= :all select-arg)
      dim
      (= :lla select-arg)
      (case dim-type
        :tensor (throw (ex-info "Can not reverse tensor indexers"
                                {:dimension dim :select-arg select-arg}))
        :classified-sequence (shape/reverse-classified-sequence dim))
      (= :tensor dim-type)
      (throw (ex-info "Only :all select types are supported on tensor dimensions"
                      {:dimension dim
                       :select-arg select-arg}))
      :else
      (do
       ;;select arg and dim are classified sequences
       (assert (and (= dim-type :classified-sequence)
                    (= select-type :classified-sequence))
               "Internal logic failure")
       ;;Carry the scalar attribute but replace anything else.
       (merge (select-keys select-arg [:scalar?])
              (shape/combine-classified-sequences
               dim select-arg))))))


(defn dimensions->simpified-dimensions
  "Given the dimensions post selection, produce a new dimension sequence combined with
  an offset that lets us know how much we should offset the base storage type.
  Simplification is important because it allows a backend to hit more fast paths.
Returns:
{:dimension-seq dimension-seq
:offset offset}"
  [dimension-seq stride-seq]
  (let [[dimension-seq strides offset]
        (reduce (fn [[dimension-seq strides offset] [dimension stride]]
                  (cond
                    (tensor? dimension)
                    [(conj dimension-seq dimension) (conj strides stride) offset]
                    (shape/classified-sequence? dimension)
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
                      (if-not (and (= 1 dimension) scalar?)
                        [(conj dimension-seq dimension)
                         (conj strides stride) new-offset]
                        ;;We keep track of offsetting but we don't add the
                        ;;element to the return value.
                        [dimension-seq strides new-offset]))
                    :else
                    (throw (ex-info "Bad dimension type"
                                    {:dimension dimension}))))
                [[] [] 0]
                (map vector dimension-seq stride-seq))
        retval

        {:dimension-seq dimension-seq
         :strides strides
         :offset offset
         :length (when (shape/direct-shape? dimension-seq)
                   (apply + 1 (map * (map (comp dec shape/shape-entry->count)
                                          dimension-seq) strides)))}]
    retval))
