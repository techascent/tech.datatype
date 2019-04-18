(ns tech.tensor.impl
    (:require [tech.datatype.protocols :as dtype-proto]
              [tech.datatype.base :as dtype-base]
              [tech.datatype.casting :as casting]
              [tech.datatype.sparse.protocols :as sparse-proto]
              [tech.datatype.sparse.reader :as sparse-reader]
              [tech.tensor.dimensions :as dims]
              [tech.tensor.dimensions.shape :as shape]
              [tech.datatype.reader :as reader]
              [tech.datatype.writer :as writer]
              [tech.datatype.functional.impl :as fn-impl]
              [tech.datatype.unary-op :as unary-op]
              [tech.datatype.binary-op :as binary-op]
              [tech.datatype.reduce-op :as reduce-op]
              [tech.datatype.boolean-op :as boolean-op]
              [tech.datatype.typecast :as typecast]
              [tech.datatype :as dtype]
              [tech.datatype.functional :as dtype-fn]
              [tech.tensor.typecast :as tens-typecast]
              [tech.tensor.protocols :as tens-proto]
              [tech.libs.blas :as blas]
              [tech.jna :as jna]
              [clojure.core.matrix.protocols :as mp]
              [clojure.core.matrix :as m]
              [clojure.core.matrix.impl.pprint :as corem-pp])
    (:import [tech.datatype IntReader
              IndexingSystem$Backward]
             [java.io Writer]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(def ^:dynamic *datatype* :float64)
(def ^:dynamic *container-type* :typed-buffer)

(defmacro with-datatype
  [dtype & body]
  `(with-bindings {#'*datatype* ~dtype}
     ~@body))


(defn default-datatype
  [& [dtype-or-nil]]
  (or dtype-or-nil *datatype*))


(defn default-container-type
  [& [container-type]]
  (or container-type *container-type*))


(defn- dimensions->index-reader
  [dimensions]
  ^IntReader (dims/->global->local dimensions))


(defn- dimensions->index-inverter
  ^IndexingSystem$Backward [dimensions]
  (dims/->local->global dimensions))


(defn simple-dimensions?
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

(defn- global-address->global-shape
  [global-address global-strides]
  (first
   (reduce (fn [[shape global-address] global-stride]
             (let [shape-idx (quot (int global-address)
                                   (int global-stride))]
               [(conj shape shape-idx)
                (rem (int global-address)
                     (int global-stride))]))
           [[] global-address]
           global-strides)))


(defmacro make-tensor-reader
  [reader-datatype datatype item-shape indexer reader sparse-reader]
  `(let [^tech.tensor.IntReader indexer# ~indexer
         data# (typecast/datatype->reader ~reader-datatype ~reader)
         ^tech.datatype.IntReader base-reader# indexer#
         shape# ~item-shape
         n-elems# (int (apply * 1 shape#))
         n-dims# (count shape#)
         strides# (dims/extend-strides shape#)]
     (reify
       ~(tens-typecast/datatype->tensor-reader-type reader-datatype)
       (read2d [reader# row# col#]
         (.read data# (.read2d indexer# row# col#)))
       (tensorRead [reader# indexes#]
         (.read data# (.tensorRead indexer# indexes#)))

       mp/PDimensionInfo
       (dimensionality [m] n-dims#)
       (get-shape [m] shape#)
       (is-scalar? [m] false)
       (is-vector? [m] true)
       (dimension-count [m dimension-number#]
         (if (<= n-dims# (int dimension-number#))
           (get shape# dimension-number#)
           (throw (ex-info "Array does not have specific dimension"
                           {:dimension-number dimension-number#
                            :shape shape#}))))
       ~(typecast/datatype->reader-type reader-datatype)
       (getDatatype [item#] ~datatype)
       (size [item#] n-elems#)
       (read [item# idx#]
         (.read data# (.read base-reader# idx#)))
       (invoke [item# arg#]
         (.read item# (int arg#)))
       (invoke [item# row# col#]
         (.read2d item# (int row#) (int col#)))
       (applyTo [item# arglist#]
         (.read data# (apply indexer# arglist#)))
       sparse-proto/PSparse
       (index-seq [item#]
         (->> (when ~sparse-reader
                (sparse-proto/index-seq ~sparse-reader))
              (map (fn [{:keys [~'global-index] :as item#}]
                     (assoc item#
                            :global-dims
                            (global-address->global-shape ~'global-index strides#))))))
       (sparse-value [item#] (sparse-proto/sparse-value ~sparse-reader))
       (sparse-ecount [item#] (sparse-proto/sparse-ecount ~sparse-reader))
       (readers [item#] (sparse-proto/readers ~sparse-reader))
       (iterables [item] (sparse-proto/iterables ~sparse-reader))

       sparse-proto/PToSparseReader
       (convertible-to-sparse-reader? [item#] (nil? ~sparse-reader))
       (->sparse-reader [item#] ~sparse-reader))))


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
  (convertible-to-nio-buffer? [item]
    (dtype-proto/nio-convertible? buffer))
  (->buffer-backing-store [item]
    (when (simple-dimensions? dimensions)
      (typecast/as-nio-buffer buffer)))


  dtype-proto/PToList
  (convertible-to-fastutil-list? [item]
    (dtype-proto/list-convertible? buffer))
  (->list-backing-store [item]
    (when (simple-dimensions? dimensions)
      (typecast/as-list buffer)))


  jna/PToPtr
  (is-jna-ptr-convertible? [item]
    (jna/ptr-convertible? buffer))
  (->ptr-backing-store [item]
    (when (simple-dimensions? dimensions)
      (jna/as-ptr buffer)))


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
      (if (= :sparse (dtype/buffer-type buffer))
        (dtype-proto/write-indexes! buffer
                                    (-> (dimensions->index-reader dimensions)
                                        (dtype-base/sub-buffer offset elem-count))
                                    (sparse-reader/const-sparse-reader
                                     value
                                     (dtype-base/get-datatype item)
                                     elem-count)
                                    {:indexes-in-order?
                                     (dims/access-increasing? dimensions)})
        (dtype-proto/set-constant! (writer/make-indexed-writer
                                    (dimensions->index-reader dimensions)
                                    buffer
                                    {})
                                   offset value elem-count))))


  dtype-proto/PWriteIndexes
  (write-indexes! [item indexes values options]
    (if (simple-dimensions? dimensions)
      (dtype-proto/write-indexes! buffer indexes values options)
      (dtype-proto/write-indexes! buffer (reader/make-indexed-reader
                                          indexes
                                          (dimensions->index-reader dimensions)
                                          {:datatype :int32})
                                  values options)))


  dtype-proto/PToReader
  (->reader-of-type [item datatype unchecked?]
    (reader/make-indexed-reader (dimensions->index-reader dimensions)
                                (dtype-proto/->reader-of-type buffer datatype unchecked?)
                                {:datatype datatype}))


  dtype-proto/PToWriter
  (->writer-of-type [item datatype unchecked?]
    (let [data-writer (dtype-proto/->writer-of-type
                       buffer datatype unchecked?)]
      (if (simple-dimensions? dimensions)
        data-writer
        (writer/make-indexed-writer (dimensions->index-reader dimensions)
                                    (dtype-proto/->writer-of-type
                                     buffer datatype unchecked?)
                                    {:datatype datatype}))))


  dtype-proto/PBufferType
  (buffer-type [item]
    (or buffer-type (dtype-proto/buffer-type buffer)))


  sparse-proto/PToSparseReader
  (convertible-to-sparse-reader? [item]
    (sparse-proto/sparse-convertible? buffer))
  (->sparse-reader [item]
    (when-let [reader (cond
                        ;;Instance checks are faster than protocol checks.
                        (or (instance? tech.datatype.sparse.protocols.PSparse buffer)
                            (satisfies? sparse-proto/PSparse buffer))
                        buffer
                        (or (instance? tech.datatype.sparse.protocols.PToSparseReader buffer)
                            (satisfies? sparse-proto/PToSparseReader buffer))
                        (sparse-proto/->sparse-reader buffer))]
      (if (simple-dimensions? dimensions)
        reader
        ;;Else we have a somewhat significant translation step from local
        ;;sparse to global sparse.
        (let [{:keys [indexes data]} (sparse-proto/readers reader)
              addr-inverter (dimensions->index-inverter dimensions)
              direct? (dims/direct? dimensions)
              raw-shape (if direct?
                          (:shape dimensions)
                          (shape/shape->count-vec (:shape dimensions)))
              broadcasting? (not= (:max-shape dimensions)
                                  raw-shape)
              access-increasing? (dims/access-increasing? dimensions)
              dense? (dims/dense? dimensions)
              index-seq (map-indexed vector indexes)
              index-seq (if (and dense? (not broadcasting?))
                          (map (fn [[data-index global-index]]
                                 [data-index (first (.localToGlobal
                                                     addr-inverter global-index))])
                               index-seq)
                          (mapcat (fn [[data-index global-index]]
                                    (->> (.localToGlobal addr-inverter global-index)
                                         (map #(vector data-index %))))))
              index-seq (if (or broadcasting? (not access-increasing?))
                          (sort-by second index-seq)
                          index-seq)]
          (sparse-reader/make-sparse-reader
           (dtype-proto/make-container :list :int32
                                       (map second index-seq)
                                       {:unchecked? true})
           (reader/make-indexed-reader (dtype-proto/make-container
                                        :list :int32 (map first index-seq)
                                        {:unchecked? true})
                                       data
                                       {})
           (dtype/ecount item))))))

  tens-proto/PToTensorReader
  (->tensor-reader-of-type [item datatype unchecked?]
    (let [sparse-data (when (sparse-proto/is-sparse? buffer)
                        (sparse-proto/->sparse-reader item))
          data-reader (dtype-proto/->reader-of-type buffer datatype unchecked?)
          indexes (dimensions->index-reader dimensions)
          item-shape (mp/get-shape item)]
      (case (casting/safe-flatten datatype)
        :int8 (make-tensor-reader :int8 datatype item-shape indexes data-reader sparse-data)
        :int16 (make-tensor-reader :int16 datatype item-shape indexes data-reader sparse-data)
        :int32 (make-tensor-reader :int32 datatype item-shape indexes data-reader sparse-data)
        :int64 (make-tensor-reader :int64 datatype item-shape indexes data-reader sparse-data)
        :float32 (make-tensor-reader :float32 datatype item-shape indexes data-reader sparse-data)
        :float64 (make-tensor-reader :float64 datatype item-shape indexes data-reader sparse-data)
        :boolean (make-tensor-reader :boolean datatype item-shape indexes data-reader sparse-data)
        :object (make-tensor-reader :object datatype item-shape indexes data-reader sparse-data)))))


(defn construct-tensor
  [buffer dims & [buffer-type]]
  (->Tensor buffer dims (or buffer-type
                            :tensor)))


(defn tensor?
  [item]
  (instance? Tensor item))


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


(defn ensure-tensor
  [item]
  (if (tensor? item)
    item
    (if-let [item-shape (dtype-base/shape item)]
      (construct-tensor item (dims/dimensions item-shape))
      (throw (ex-info "Cannot construct tensor from item with no shape." {})))))


(defn tensor->buffer
  [tens]
  (:buffer tens))


(defn tensor->dimensions
  [tens]
  (:dimensions tens))


(defn mutable?
  "Does this tensor have the ability to be written to."
  [tens]
  (satisfies? dtype-proto/PToWriter (tensor->buffer tens)))


(defn tensor->base-buffer-type
  [tens]
  (if (tensor? tens)
    (assoc tens :buffer-type
           (dtype/buffer-type
            (tensor->buffer tens)))
    tens))

(defn ->tensor
  [data & {:keys [datatype container-type]
           :as options}]
  (let [data-shape (dtype/shape data)
        datatype (default-datatype datatype)
        container-type (default-container-type container-type)
        n-elems (apply * 1 data-shape)]
    (construct-tensor
     (first
      (dtype/copy-raw->item!
       data
       (dtype/make-container container-type datatype n-elems options)
       0 options))
     (dims/dimensions data-shape))))


(defn new-tensor
  [shape & {:keys [datatype container-type]
            :as options}]
  (let [datatype (default-datatype datatype)
        container-type (default-container-type container-type)
        n-elems (apply * 1 shape)]
    (construct-tensor
     (dtype/make-container container-type datatype n-elems options)
     (dims/dimensions shape))))


(defn clone
  [tens & {:keys [datatype
                  container-type]}]
  (let [datatype (or datatype (dtype/get-datatype tens))
        container-type (default-container-type (or container-type
                                                (dtype/container-type tens)))
        new-buffer (if (satisfies? dtype-proto/PPrototype (tensor->buffer tens))
                     (dtype/from-prototype (tensor->buffer tens)
                                           :datatype datatype
                                           :shape (dtype/shape
                                                   (tensor->buffer tens)))
                     (dtype/make-container (default-container-type container-type)
                                           datatype
                                           (dtype/ecount tens)))
        new-tens (construct-tensor
                  new-buffer
                  (dims/dimensions (dtype/shape tens)))]
    (dtype/copy! tens new-tens)))


(defn tensor-force
  "Ensure any delayed operations happen for this and reads from this tensor
  happen reasonably fast.  For sparse this probably means cloning."
  [tens]
  (let [buffer-type (dtype/buffer-type (:buffer tens))
        new-tens (if (= :sparse buffer-type)
                   (construct-tensor
                    (sparse-proto/->sparse-reader tens)
                    (dims/dimensions (dtype/shape tens)))
                   ;;force a potentially deep reader chain.
                   (if (or (not (simple-dimensions? (:dimensions tens)))
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
  (let [tens (ensure-tensor tens)]
    (assoc tens :dimensions
           (dims/rotate (tensor->dimensions tens)
                        (mapv #(* -1 (long %)) rotate-vec)))))


(defn reshape
  [tens new-shape]
  (let [tens (ensure-tensor tens)
        new-dims (dims/in-place-reshape (:dimensions tens)
                                        new-shape)]
    (construct-tensor
     (dtype/sub-buffer (tensor->buffer tens)
                       0 (dims/buffer-ecount new-dims))
     new-dims)))


(defn transpose
  [tens transpose-vec]
  (let [tens (ensure-tensor tens)]
    (update tens :dimensions dims/transpose transpose-vec)))


(defn select
  [tens & args]
  (let [tens (ensure-tensor tens)
        {new-dims :dims
         buf-offset :elem-offset
         buf-len :buffer-length}
        (apply dims/select (:dimensions tens) args)]
    (construct-tensor (-> (tensor->buffer tens)
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


(defmethod unary-op/unary-reader-map :tensor
  [options un-op item]
  (construct-tensor (unary-op/unary-reader-map
                     options un-op
                     (tensor->base-buffer-type item))
                    (dims/dimensions (dtype/shape item))))


(defmethod boolean-op/boolean-unary-reader-map :tensor
  [options bool-op item]
  (construct-tensor (boolean-op/boolean-unary-reader-map
                     options bool-op
                     (tensor->base-buffer-type item))
                    (dims/dimensions (dtype/shape item))))


(defn default-tensor-binary-reader-map
  "Anything times a tensor returns a thing in the shape of
  the tensor.  ecounts must match."
  [options bin-op lhs rhs]
  (when-not (= (dtype-base/ecount lhs)
               (dtype-base/ecount rhs))
    (throw (ex-info "Ecounts don't match" {})))
  (let [lhs-shape (dtype-base/shape lhs)
        lhs-tensor? (tensor? lhs)
        lhs (if (tensor? lhs)
              (tensor->base-buffer-type lhs)
              lhs)
        rhs-shape (dtype-base/shape rhs)
        rhs (if (tensor? rhs)
              (tensor->base-buffer-type rhs)
              rhs)]
    (construct-tensor
     (binary-op/binary-reader-map options bin-op lhs rhs)
     (if lhs-tensor?
       (dims/dimensions lhs-shape)
       (dims/dimensions rhs-shape)))))

;; Next up
(defmethod binary-op/binary-reader-map [:dense :tensor]
  [options bin-op lhs rhs]
  (default-tensor-binary-reader-map options bin-op lhs rhs))

(defmethod binary-op/binary-reader-map [:tensor :dense]
  [options bin-op lhs rhs]
  (default-tensor-binary-reader-map options bin-op lhs rhs))

(defmethod binary-op/binary-reader-map [:tensor :tensor]
  [options bin-op lhs rhs]
  (default-tensor-binary-reader-map options bin-op lhs rhs))

(defmethod binary-op/binary-reader-map [:sparse :tensor]
  [options bin-op lhs rhs]
  (default-tensor-binary-reader-map options bin-op lhs rhs))


(defmethod binary-op/binary-reader-map [:tensor :sparse]
  [options bin-op lhs rhs]
  (default-tensor-binary-reader-map options bin-op lhs rhs))


(defn default-tensor-binary-boolean-reader-map
  "Anything times a tensor returns a thing in the shape of
  the tensor.  ecounts must match."
  [options bin-op lhs rhs]
  (when-not (= (dtype-base/ecount lhs)
               (dtype-base/ecount rhs))
    (throw (ex-info "Ecounts don't match" {})))
  (let [lhs-shape (dtype-base/shape lhs)
        lhs-tensor? (tensor? lhs)
        lhs (if (tensor? lhs)
              (tensor->base-buffer-type lhs)
              lhs)
        rhs-shape (dtype-base/shape rhs)
        rhs (if (tensor? rhs)
              (tensor->base-buffer-type rhs)
              rhs)]
    (construct-tensor
     (boolean-op/boolean-binary-reader-map options bin-op lhs rhs)
     (if lhs-tensor?
       (dims/dimensions lhs-shape)
       (dims/dimensions rhs-shape)))))

;; Next up
(defmethod boolean-op/boolean-binary-reader-map [:dense :tensor]
  [options bin-op lhs rhs]
  (default-tensor-binary-boolean-reader-map options bin-op lhs rhs))

(defmethod boolean-op/boolean-binary-reader-map [:tensor :dense]
  [options bin-op lhs rhs]
  (default-tensor-binary-boolean-reader-map options bin-op lhs rhs))

(defmethod boolean-op/boolean-binary-reader-map [:tensor :tensor]
  [options bin-op lhs rhs]
  (default-tensor-binary-boolean-reader-map options bin-op lhs rhs))

(defmethod boolean-op/boolean-binary-reader-map [:sparse :tensor]
  [options bin-op lhs rhs]
  (default-tensor-binary-boolean-reader-map options bin-op lhs rhs))


(defmethod boolean-op/boolean-binary-reader-map [:tensor :sparse]
  [options bin-op lhs rhs]
  (default-tensor-binary-boolean-reader-map options bin-op lhs rhs))





(defn ->core-matrix
  [tensor]
  (let [retval (m/new-array :vectorz (dtype-base/shape tensor))
        double-data (mp/as-double-array retval)]
    (dtype-base/copy! double-data tensor)
    retval))


(defn ->core-matrix-vector
  [tensor]
  (m/as-vector (->core-matrix tensor)))


(defn ->jvm
  "Conversion to storage that is efficient for the jvm.
  Base storage is either jvm-array or persistent-vector."
  [item & {:keys [datatype base-storage]
           :or {base-storage :persistent-vector}}]
  ;;Get the data off the device
  (let [item-shape (dtype-base/shape item)
        item-ecount (dtype-base/ecount item)
        column-len (long (last item-shape))
        n-columns (quot item-ecount column-len)
        datatype (or datatype (dtype-base/get-datatype item))
        data-array (dtype-proto/->reader-of-type
                    item datatype true)
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
  (format "#tech.tensor<%s>%s\n%s"
          (name (dtype/get-datatype tens))
          (dtype/shape tens)
          (corem-pp/pm (->jvm tens))))


(defmethod print-method Tensor
  [tens w]
  (.write ^Writer w (tensor->string tens)))


(defmethod dtype-proto/copy! [:tensor :dense]
  [dst src options]
  (dtype-proto/copy! dst (tensor->base-buffer-type src) options)
  dst)


(defmethod dtype-proto/copy! [:tensor :sparse]
  [dst src options]
  (dtype-proto/copy! dst (tensor->base-buffer-type src) options)
  dst)


(defmethod dtype-proto/copy! [:dense :tensor]
  [dst src options]
  (dtype-proto/copy! (tensor->base-buffer-type dst) src options)
  dst)


(defmethod dtype-proto/copy! [:sparse :tensor]
  [dst src options]
  (dtype-proto/copy! (tensor->base-buffer-type dst) src options)
  dst)


(defmethod dtype-proto/copy! [:tensor :tensor]
  [dst src options]
  (dtype-proto/copy! (tensor->base-buffer-type dst)
                     (tensor->base-buffer-type src) options)
  dst)


(defmacro impl-dot-product
  [match-criteria]
  `(defmethod reduce-op/dot-product ~match-criteria
     [options# lhs# rhs# bin-op# reduce-op#]
     (when (or (not= 1 (count (dtype-base/shape lhs#)))
               (not= 1 (count (dtype-base/shape rhs#))))
       (throw (ex-info "Dot product called incorrectly"
                       {:lhs-shape (dtype-base/shape lhs#)
                        :rhs-shape (dtype-base/shape rhs#)})))
     (reduce-op/dot-product
      options#
      (tensor->base-buffer-type lhs#)
      (tensor->base-buffer-type rhs#)
      bin-op#
      reduce-op#)))


(def all-tensor-combos
  [[:tensor :tensor]
   [:tensor :dense]
   [:dense :tensor]
   [:tensor :sparse]
   [:sparse :tensor]])


(defmacro impl-all-tensor-combos
  [target-macro]
  `(do
     ~@(->> all-tensor-combos
            (map (fn [combo]
                   `(~target-macro ~combo))))))


(impl-all-tensor-combos impl-dot-product)



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
        new-tens (construct-tensor
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
        nio-access? (dtype-proto/as-nio-buffer tens)
        tens-shape (dtype/shape tens)]
    (if (or any-offsets?
            (not nio-access?)
            ;;Matrixes can only have a minimum stride of 1.
            ;;Vectors can have any stride, however.
            (and (= 2 (count tens-shape))
                 (not= 1 (apply min (get-in tens [:dimensions :strides])))))
      (clone tens)
      tens)))


(defmethod matrix-matrix-dispatch [:dense :dense :* :+]
  [alpha lhs rhs bin-op reduce-op options]
  (let [lhs-dtype (dtype/get-datatype lhs)
        rhs (ensure-tensor rhs)]
    (if (and (or (= lhs-dtype :float32)
                 (= lhs-dtype :float64))
             (blas/has-blas?))
      (let [[lhs-shape rhs-shape] (mmul-check lhs rhs)
            ;;It is worth it to copy because copy is a O(N) while matrix*matrix is
            ;;O(N^3).
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
            lhs-max-stride (int (apply max lhs-strides))
            rhs-max-stride (int (apply max rhs-strides))
            c-max-stride (int (apply max (get-in C [:dimensions :strides])))
            lhs-trans? (= lhs-min-stride (first lhs-strides))
            rhs-trans? (= rhs-min-stride (first rhs-strides))
            gemv? (= 1 (second rhs-shape))]
        (if gemv?
          ((case lhs-dtype
             :float32 blas/cblas_sgemv
             :float64 blas/cblas_dgemv)
           :row-major lhs-trans?
           (first lhs-shape)  (second lhs-shape)
           alpha (:buffer lhs) lhs-max-stride (:buffer rhs) rhs-min-stride
           beta (:buffer C) 1)
          ((case lhs-dtype
             :float32 blas/cblas_sgemm
             :float64 blas/cblas_dgemm)
           :row-major lhs-trans? rhs-trans?
           (first lhs-shape) (first rhs-shape) (second lhs-shape)
           alpha (:buffer lhs) lhs-max-stride (:buffer rhs) rhs-max-stride
           beta (:buffer C) c-max-stride))
        C)
      (default-matrix-matrix alpha lhs rhs bin-op reduce-op options))))
