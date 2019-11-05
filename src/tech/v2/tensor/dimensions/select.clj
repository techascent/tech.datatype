(ns tech.v2.tensor.dimensions.select
  "Selecting subsets from a larger set of dimensions leads to its own algebra."
  (:require [tech.v2.tensor.dimensions.shape :as shape]
            [tech.v2.tensor.utils :refer [when-not-error]]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.typecast :as typecast])
  (:import [tech.v2.datatype LongReader]))

(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn- expand-dimension
  [dim]
  (cond (number? dim)
        {:type :+
         :min-item 0
         :max-item (- (long dim) 1)}
        (shape/classified-sequence? dim) dim
        (or (sequential? dim)
            (dtype/reader? dim)) (shape/classify-sequence dim)
        :else
        (throw (ex-info "Failed to recognize dimension type"
                        {:dimension dim}))))


(defn- expand-select-arg
  [select-arg]
  (cond
    (number? select-arg) (shape/classify-sequence select-arg)
    (shape/classified-sequence? select-arg) select-arg
    (instance? clojure.lang.LongRange select-arg)
    (shape/classify-sequence select-arg)
    (dtype/reader? select-arg)
    select-arg
    (= :all select-arg) select-arg
    (= :lla select-arg) select-arg
    :else
    (throw (ex-info "Unrecognized select argument type"
                    {:select-arg select-arg}))))


(defn apply-select-arg-to-dimension
  "Given a dimension and select argument, create a new dimension with
the selection applied."
  [dim select-arg]
  ;;Dim is now a map or a reader
  (let [dim (expand-dimension dim)
        ;;Select arg is now a map, a keyword, or a reader
        select-arg (expand-select-arg select-arg)
        dim-type (cond (dtype/reader? dim) :reader
                       (shape/classified-sequence? dim)  :classified-sequence
                       :else (throw (Exception. "Unrecognized dim type.")))
        select-type (cond (dtype/reader? select-arg) :reader
                          (shape/classified-sequence? select-arg) :classified-sequence
                          (keyword? select-arg) :keyword
                          :else (throw (Exception. "Unrecognized select type.")))]
    (case select-type
      :reader
      (case dim-type
        :reader (dtype/indexed-reader select-arg dim {:datatype :int64})
        :classified-sequence
        (if (and (= :+ (:type dim))
                 (= 0 (long (:min-item dim))))
          select-arg
          (dtype/indexed-reader select-arg
                                (shape/classified-sequence->reader dim)
                                {:datatype :int64})))
      :keyword
      (case select-arg
        :all dim
        :lla (case dim-type
               :reader (let [src-reader (typecast/datatype->reader :int64 dim)
                             n-elems (.lsize src-reader)]
                         (reify LongReader
                           (lsize [rdr] n-elems)
                           (read [rdr idx]
                             (- n-elems idx 1))))
               :classified-sequence (shape/reverse-classified-sequence dim)))
      :classified-sequence
      (case dim-type
        :reader
        (dtype/indexed-reader (shape/classified-sequence->reader select-arg)
                              dim
                              {:datatype :int64})
        :classified-sequence
        (shape/combine-classified-sequences dim select-arg)))))


(defn dimensions->simpified-dimensions
  "Given the dimensions post selection, produce a new dimension sequence combined with
  an offset that lets us know how much we should offset the base storage type.
  Simplification is important because it allows a backend to hit more fast paths.
Returns:
{:dimension-seq dimension-seq
:offset offset}"
  [dimension-seq stride-seq dim-offset-seq]
  (let [[dimension-seq strides dim-offsets offset]
        (reduce (fn [[dimension-seq strides dim-offsets offset]
                     [dimension stride dim-offset]]
                  (let [dimension (if (and (dtype/reader? dimension)
                                           (= 1 (dtype/ecount dimension)))

                                    (let [dim-val (dtype/get-value dimension 0)]
                                      {:type :+ :min-item dim-val :max-item dim-val})
                                    dimension)]
                    (cond
                      (dtype/reader? dimension)
                      [(conj dimension-seq dimension)
                       (conj strides stride)
                       (conj dim-offsets dim-offset)
                       offset]
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
                      :else
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
