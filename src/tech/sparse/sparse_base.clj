(ns tech.sparse.sparse-base
  (:require [tech.datatype.reader :as reader]
            [tech.datatype :as dtype]
            [tech.datatype.binary-search :as dtype-search]
            [tech.datatype.unary-op :as unary-op]
            [tech.datatype.binary-op :as binary-op]
            [tech.datatype.reduce-op :as reduce-op]
            [tech.datatype.boolean-op :as boolean-op]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting :as casting]
            [tech.datatype.typecast :as typecast]
            [tech.datatype.base :as dtype-base]
            [tech.sparse.protocols :as sparse-proto]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.functional.impl :as impl]
            [clojure.core.matrix.protocols :as mp]))


(defrecord IndexSeqRec [^long data-index ^long global-index])


(defn get-index-seq
  "Given the offset and stride of the buffer
  return an index sequence that contains a sequence of
  tuples that contain"
  [data-offset data-stride index-buf]
  (let [data-offset (int data-offset)
        data-stride (int data-stride)
        n-elems (dtype/ecount index-buf)
        [first-idx adj-index-buf]
        (if (= 0 data-offset)
          [0 index-buf]
          (let [off-idx (second (dtype-search/binary-search index-buf data-offset {}))]
            [off-idx (dtype/sub-buffer index-buf off-idx)]))
        first-idx (int first-idx)
        index-seq (if (= 0 data-offset)
                    (dtype/->reader-of-type index-buf :int32)
                    (unary-op/unary-reader-map
                     {:datatype :int32}
                     (unary-op/make-unary-op :int32 (- arg data-offset))
                     adj-index-buf))
        index-seq (map ->IndexSeqRec
                       (range first-idx n-elems)
                       index-seq)]
    (if (= 1 data-stride)
      index-seq
      (->> index-seq
           ;; only return indexes which are commensurate with the stride.
           (map (fn [record]
                  (let [global-index (int (:global-index record))]
                    (when (= 0 (rem global-index data-stride))
                      (assoc record :global-index
                             (quot global-index data-stride))))))
           (remove nil?)))))


(defn index-seq->readers
  [index-seq & [data-reader]]
  (merge {:indexes (impl/->reader (map :global-index index-seq) :int32)}
         (when data-reader
           {:data (reader/make-indexed-reader
                   (impl/->reader (map :data-index index-seq) :int32)
                   (impl/->reader data-reader)
                   {})})))


(defn index-seq->iterables
  [index-seq & [data-reader]]
  (merge {:indexes (map :global-index index-seq)}
         (when data-reader
           {:data (reader/make-iterable-indexed-iterable
                   (map :data-index index-seq)
                   (impl/->iterable data-reader))})))


(defn sparse-zero-value
  [datatype]
  (case (casting/host-flatten datatype)
    :int8 (casting/datatype->sparse-value :int8)
    :int16 (casting/datatype->sparse-value :int16)
    :int32 (casting/datatype->sparse-value :int32)
    :int64 (casting/datatype->sparse-value :int64)
    :float32 (casting/datatype->sparse-value :float32)
    :float64 (casting/datatype->sparse-value :float64)
    :boolean (casting/datatype->sparse-value :boolean)
    :object (casting/datatype->sparse-value :object)))


(declare make-sparse-reader)


(defn naive-index-iterable->index-seq
  [index-iterable]
  (map-indexed (fn [data-idx global-idx]
                 {:data-index data-idx
                  :global-index global-idx})
               index-iterable))


(defmacro make-indexed-data-reader
  [datatype]
  `(fn [index-reader# data-reader# n-elems# zero-val#]
     (let [local-data-reader# (typecast/datatype->reader ~datatype data-reader# true)
           n-elems# (int n-elems#)
           zero-val# (casting/datatype->cast-fn :unknown ~datatype zero-val#)]
       (reify
         ~(typecast/datatype->reader-type datatype)
         (getDatatype [reader#] (dtype-proto/get-datatype data-reader#))
         (size [reader#] n-elems#)
         (read [reader# idx#]
           (let [[found?# data-idx#] (dtype-search/binary-search index-reader# (int idx#) {})]
             (if found?#
               (.read local-data-reader# (int data-idx#))
               zero-val#)))
         (invoke [reader# idx#]
           (.read reader# (int idx#)))
         (iterator [reader#] (typecast/reader->iterator reader#))
         sparse-proto/PSparse
         (index-seq [reader#]
           (naive-index-iterable->index-seq index-reader#))
         (zero-value [reader#] zero-val#)
         (index-reader [reader#] index-reader#)
         (set-stride [item# new-stride#]
           (when-not (= 0 (rem n-elems# new-stride#))
             (throw (ex-info (format "Element count %s is not commensurate with stride %s."
                                     n-elems# new-stride#)
                             {})))
           (if (= 1 (int new-stride#))
             item#
             (let [new-idx-seq# (get-index-seq 0 new-stride# index-reader#)
                   {indexes# :indexes
                    data# :data} (index-seq->readers new-idx-seq# data-reader#)]
               (make-sparse-reader indexes# data# (quot n-elems# (int new-stride#))
                                   :zero-value zero-val#))))
         (stride [item#] 1)
         (find-index [item# target-idx#]
           (dtype-search/binary-search index-reader# (int target-idx#) {}))
         (readers [item#]
           {:index-reader index-reader#
            :data-reader data-reader#})
         (iterables [item#]
           (sparse-proto/readers item#))
         sparse-proto/PSparseData
         (data-reader [item#] data-reader#)
         sparse-proto/PToSparseReader
         (->sparse-reader [item#] item#)
         dtype-proto/PBuffer
         (sub-buffer [item# offset# length#]
           (when-not (<= (+ offset# length#)
                         n-elems#)
             (throw (ex-info (format "Sub-buffer out of range: %s > %s"
                                     (+ offset# length#)
                                     n-elems#)
                             {})))
           (let [start-elem# (int (second (dtype-search/binary-search index-reader# offset# {})))
                 end-elem# (int (second (dtype-search/binary-search index-reader# (+ offset# length#) {})))
                 sub-len# (- end-elem# start-elem#)
                 new-idx-reader# (->> (dtype-proto/sub-buffer index-reader# start-elem# sub-len#)
                                      (unary-op/unary-reader-map
                                       {:datatype :int32}
                                       (unary-op/make-unary-op :int32 (- ~'arg offset#))))
                 new-data-reader# (dtype-proto/sub-buffer data-reader# start-elem# sub-len#)]
             (make-sparse-reader new-idx-reader# new-data-reader# length#
                                 :zero-value zero-val#)))
         ;;These two queries are irrelevant with readers
         (alias? [lhs# rhs#] false)
         (partially-alias? [lhs# rhs#] false)))))


(defmacro make-indexed-reader-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-indexed-data-reader ~dtype)])]
        (into {})))


(def indexed-reader-table (make-indexed-reader-table))


(defn make-sparse-reader
  [index-reader data-reader n-elems & {:keys [datatype
                                              zero-value]}]
  (let [datatype (casting/safe-flatten
                  (or datatype (dtype-base/get-datatype data-reader)))
        create-fn (get indexed-reader-table datatype)
        zero-value (or zero-value (sparse-zero-value datatype))]
    (create-fn (impl/->reader index-reader datatype)
               (impl/->reader data-reader datatype)
               n-elems
               zero-value)))


(defn sparse-unary-map
  [options un-op sparse-item]
  (make-sparse-reader (sparse-proto/index-reader sparse-item)
                      (unary-op/apply-unary-op {} un-op (sparse-proto/data-reader sparse-item))
                      (dtype-base/ecount sparse-item)
                      :zero-value (un-op (sparse-proto/zero-value sparse-item))))


(defn sparse-boolean-unary-map
  [options un-op sparse-item]
  (make-sparse-reader (sparse-proto/index-reader sparse-item)
                      (boolean-op/apply-unary-op {} un-op (sparse-proto/data-reader sparse-item))
                      (dtype-base/ecount sparse-item)
                      :zero-value (un-op (sparse-proto/zero-value sparse-item))
                      :datatype :boolean))


(defn sparse-binary-union-map
  [options bin-op sparse-lhs sparse-rhs]
  ()
  (make-sparse-reader (sparse-proto/index-reader sparse-item)
                      (unary-op/apply-unary-op {} un-op (sparse-proto/data-reader sparse-item))
                      (dtype-base/ecount sparse-item)
                      :zero-value (un-op (sparse-proto/zero-value sparse-item))))


;; (defn sparse-boolean-unary-map
;;   [options un-op sparse-item]
;;   (make-sparse-reader (sparse-proto/index-reader sparse-item)
;;                       (boolean-op/apply-unary-op {} un-op (sparse-proto/data-reader sparse-item))
;;                       (dtype-base/ecount sparse-item)
;;                       :zero-value (un-op (sparse-proto/zero-value sparse-item))
;;                       :datatype :boolean))
