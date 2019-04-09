(ns tech.tensor.impl
    (:require [tech.datatype.protocols :as dtype-proto]
              [tech.datatype.base :as dtype-base]
              [tech.datatype.sparse.protocols :as sparse-proto]
              [tech.datatype.sparse.reader :as sparse-reader]
              [tech.tensor.dimensions :as dims]
              [tech.tensor.dimensions.shape :as shape]
              [tech.datatype.reader :as reader]
              [tech.datatype.writer :as writer]
              [tech.datatype.functional.impl :as fn-impl]
              [tech.datatype.unary-op :as unary-op]
              [tech.datatype.unary-op :as binary-op]
              [tech.datatype.unary-op :as reduce-op]
              [tech.datatype.unary-op :as boolean-op]

              [tech.datatype :as dtype]
              [clojure.core.matrix.protocols :as mp]
              [clojure.core.matrix :as m]
              [clojure.core.matrix.impl.pprint :as corem-pp])
    (:import [tech.datatype IntReader
              IndexingSystem$Backward]
             [java.io Writer]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(def ^:dynamic *datatype* :float64)

(defmacro with-datatype
  [dtype & body]
  `(with-bindings {#'*datatype* ~dtype}
     ~@body))


(defn datatype
  [dtype-or-nil]
  (or dtype-or-nil *datatype*))


(defn- dimensions->index-reader
  [dimensions]
  ^IntReader (dims/->global->local dimensions))


(defn- dimensions->index-inverter
  ^IndexingSystem$Backward [dimensions]
  (dims/->local->global dimensions))


(defn- simple-dimensions?
  [dimensions]
  (and (dims/direct? dimensions)
       (dims/access-increasing? dimensions)
       (dims/dense? dimensions)
       (or (nil? (:max-shape dimensions))
           (= (:max-shape dimensions)
              (shape/shape->count-vec (:shape dimensions))))))


(defn- simple-vector-dimensions?
  [dimensions]
  (and (simple-dimensions? dimensions)
       (= 1 (count (:shape dimensions)))))


(defrecord Tensor [buffer dimensions buffer-type]
  dtype-proto/PDatatype
  (get-datatype [item] (dtype-base/get-datatype buffer))


  mp/PElementCount
  (element-count [item] (dims/ecount dimensions))


  mp/PDimensionInfo
  (dimensionality [m] (count (mp/get-shape m)))
  (get-shape [m] (dims/shape dimensions))
  (is-scalar? [m] false)
  (is-vector? [m] true)
  (dimension-count [m dimension-number]
    (let [shape (mp/get-shape m)]
      (if (<= (count shape) (long dimension-number))
        (get shape dimension-number)
        (throw (ex-info "Array does not have specific dimension"
                        {:dimension-number dimension-number
                         :shape shape})))))


  dtype-proto/PToNioBuffer
  (->buffer-backing-store [item]
    (when (and (simple-dimensions? dimensions)
               (satisfies? dtype-proto/PToNioBuffer buffer))
      (dtype-proto/->buffer-backing-store buffer)))


  dtype-proto/PToList
  (->list-backing-store [item]
    (when (and (simple-dimensions? dimensions)
               (satisfies? dtype-proto/PToList buffer))
      (dtype-proto/->list-backing-store buffer)))


  dtype-proto/PToArray
  (->sub-array [item]
    (when (and (simple-dimensions? dimensions)
               (satisfies? dtype-proto/PToArray buffer))
      (dtype-proto/->sub-array buffer)))

  (->array-copy [item]
    (if (and (simple-dimensions? dimensions)
             (satisfies? dtype-proto/PToArray buffer))
      (dtype-proto/->array-copy buffer)
      (dtype-proto/->array-copy (dtype-proto/->writer-of-type
                                 item
                                 (dtype-proto/get-datatype item)
                                 true))))


  dtype-proto/PSetConstant
  (set-constant! [item offset value elem-count]
    (if (simple-dimensions? dimensions)
      (dtype-proto/set-constant! buffer offset value elem-count)
      (dtype-proto/set-constant! (writer/make-indexed-writer
                                  (dimensions->index-reader dimensions)
                                  buffer)
                                 offset value elem-count)))


  dtype-proto/PWriteIndexes
  (write-indexes! [item indexes values options]
    (if (simple-dimensions? dimensions)
      (dtype-proto/write-indexes! buffer indexes values options)
      (dtype-proto/write-indexes! buffer (reader/make-indexed-reader
                                          indexes
                                          (dimensions->index-reader dimensions))
                                  values options)))


  dtype-proto/PToReader
  (->reader-of-type [item datatype unchecked?]
    (reader/make-indexed-reader (dimensions->index-reader dimensions)
                                (dtype-proto/->reader-of-type buffer datatype unchecked?)
                                {:datatype datatype}))


  dtype-proto/PToWriter
  (->writer-of-type [item datatype unchecked?]
    (writer/make-indexed-writer (dimensions->index-reader dimensions)
                                (dtype-proto/->writer-of-type buffer datatype unchecked?)
                                {:datatype datatype}))


  dtype-proto/PBufferType
  (buffer-type [item]
    (or buffer-type (dtype-proto/buffer-type buffer)))


  sparse-proto/PToSparseReader
  (->sparse-reader [item]
    (when-let [reader (cond
                        (satisfies? sparse-proto/PSparse buffer)
                        buffer
                        (satisfies? sparse-proto/PToSparseReader buffer)
                        (sparse-proto/->sparse-reader buffer))]
      (if (simple-dimensions? dimensions)
        reader
        ;;Else we have a somewhat significant translation step from local
        ;;sparse to global sparse.
        (let [{:keys [indexes data]} (sparse-proto/readers reader)
              addr-inverter (dimensions->index-inverter dimensions)
              index-seq (->> indexes
                             (map-indexed vector)
                             (mapcat (fn [[data-index global-index]]
                                       (->> (.localToGlobal addr-inverter global-index)
                                            (map #(vector data-index %)))))
                             (sort-by second))]
          (sparse-reader/make-sparse-reader
           (dtype-proto/make-container :list :int32
                                       (map second index-seq)
                                       {:unchecked? true})
           (reader/make-indexed-reader (dtype-proto/make-container
                                        :list :int32 (map first index-seq)
                                        {:unchecked? true})
                                       reader)))))))


(defn construct-tensor
  [buffer dims & [buffer-type]]
  (->Tensor buffer dims (or buffer-type
                            :tensor)))


(defn tensor->buffer
  [tens]
  (:buffer tens))


(defn tensor->dimensions
  [tens]
  (:dimensions tens))


(defn mutable?
  [tens]
  (satisfies? dtype-proto/PToWriter (tensor->buffer tens)))


(defn tensor?
  [item]
  (instance? Tensor item))



(defmethod unary-op/unary-reader-map :tensor
  [options un-op item]
  (construct-tensor (unary-op/unary-reader-map
                     options un-op
                     (assoc item :buffer-type
                            (dtype/buffer-type (tensor->buffer item))))
                    (dims/dimensions (dtype/shape item))))



;; Next up
;; (defmethod binary-op/binary-reader-map [:dense :tensor]
;;   [options un-op item]
;;   (construct-tensor (unary-op/unary-reader-map
;;                      options un-op
;;                      (assoc item :buffer-type
;;                             (dtype/buffer-type (tensor->buffer item))))
;;                     (dims/dimensions (dtype/shape item))))


(defn to-core-matrix
  [tensor]
  (let [retval (m/new-array :vectorz (dtype-base/shape tensor))
        double-data (mp/as-double-array retval)]
    (dtype-base/copy! double-data tensor)
    retval))


(defn to-core-matrix-vector
  [tensor]
  (m/as-vector (to-core-matrix tensor)))


(defn ->jvm
  "Conversion to storage that is efficient for the jvm.
  Base storage is either jvm-array or persistent-vector."
  [item & {:keys [datatype base-storage]
           :or {datatype :float64
                base-storage :persistent-vector}}]
  ;;Get the data off the device
  (let [item-shape (dtype-base/shape item)
        item-ecount (dtype-base/ecount item)
        column-len (long (last item-shape))
        n-columns (quot item-ecount column-len)
        data-array (dtype-proto/->reader-of-type item (dtype-base/get-datatype item) true)
        base-data
        (->> (range n-columns)
             (map (fn [col-idx]
                    (let [col-offset (* column-len (long col-idx))]
                      (case base-storage
                        :jvm-array
                        (let [retval (dtype/make-array-of-type datatype
                                                               column-len)]
                          (dtype/copy! data-array col-offset
                                       retval 0 column-len {:unchecked? true}))
                        :persistent-vector
                        (mapv #(dtype/get-value data-array (+ (long %1)
                                                              col-offset))
                              (range column-len)))))))
        partitionv (fn [& args]
                     (map vec (apply partition args)))
        partition-shape (->> (rest item-shape)
                             drop-last
                             reverse)]
    (if (> (count item-shape) 1)
      (->> partition-shape
           (reduce (fn [retval part-value]
                     (partitionv part-value retval))
                   base-data)
           vec)
      (first base-data))))


(defn tensor->string
  ^String [tens & {:keys [print-datatype]
                   :or {print-datatype :float64}}]
  (format "#tech.tensor.impl.Tensor<%s>%s\n%s"
          (name (dtype/get-datatype tens))
          (dtype/shape tens)
          (corem-pp/pm (->jvm tens :datatype print-datatype))))


(defmethod print-method Tensor
  [tens w]
  (.write ^Writer w (tensor->string tens)))
