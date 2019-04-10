(ns tech.datatype.sparse.reader
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.reader :as reader]
            [tech.datatype.iterator :as iterator]
            [tech.datatype.casting :as casting]
            [tech.datatype.typecast :as typecast]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.sparse.protocols :as sparse-proto]
            [tech.datatype.binary-search :as dtype-search]
            [tech.datatype.unary-op :as unary-op]
            [tech.datatype.argtypes :as argtypes]
            [tech.datatype.protocols.impl
             :refer [safe-get-datatype]]))


(declare ->reader const-sparse-reader make-sparse-reader)


(defrecord IndexSeqRec [^long data-index ^long global-index])


(defn get-index-seq
  "Given the offset and stride of the buffer
  return an index sequence that contains a sequence of
  tuples that contain"
  [data-stride index-iterable]
  (let [data-stride (int data-stride)
        index-seq (->> (dtype-proto/->iterable-of-type index-iterable :int32 false)
                       (map-indexed #(->IndexSeqRec %1 %2)))]

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
  (merge {:indexes (->reader (map :global-index index-seq) :int32)}
         (when data-reader
           {:data (reader/make-indexed-reader
                   (->reader (map :data-index index-seq) :int32)
                   (->reader data-reader)
                   {})})))


(defn index-seq->iterables
  [index-seq & [data-reader]]
  (merge {:indexes (map :global-index index-seq)}
         (when data-reader
           {:data (reader/make-iterable-indexed-iterable
                   (map :data-index index-seq)
                   (iterator/->iterable data-reader))})))


(defn make-sparse-value
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



(defmacro make-indexed-data-reader
  [datatype]
  `(fn [index-reader# data-reader# n-elems# zero-val#]
     (let [local-data-reader# (typecast/datatype->reader ~datatype data-reader# true)
           n-elems# (int n-elems#)
           idx-count# (dtype-base/ecount index-reader#)
           zero-val# (casting/datatype->cast-fn :unknown ~datatype zero-val#)]
       (reify
         ~(typecast/datatype->reader-type datatype)
         (getDatatype [reader#] (dtype-proto/get-datatype data-reader#))
         (size [reader#] n-elems#)
         (read [reader# idx#]
           (let [[found?# data-idx#] (dtype-search/binary-search
                                      index-reader# (int idx#) {})]
             (if found?#
               (.read local-data-reader# (int data-idx#))
               zero-val#)))
         (invoke [reader# idx#]
           (.read reader# (int idx#)))
         (iterator [reader#] (typecast/reader->iterator reader#))
         dtype-proto/PToBackingStore
         (->backing-store-seq [item#]
           (concat (dtype-proto/->backing-store-seq index-reader#)
                   (dtype-proto/->backing-store-seq data-reader#)))
         sparse-proto/PSparse
         (index-seq [reader#]
           (get-index-seq 1 index-reader#))
         (sparse-value [reader#] zero-val#)
         (sparse-ecount [reader#] (- n-elems# idx-count#))
         (set-stride [item# new-stride#]
           (when-not (= 0 (rem n-elems# new-stride#))
             (throw (ex-info (format
                              "Element count %s is not commensurate with stride %s."
                              n-elems# new-stride#)
                             {})))
           (if (= 1 (int new-stride#))
             item#
             (let [new-idx-seq# (get-index-seq new-stride# index-reader#)
                   {indexes# :indexes
                    data# :data} (index-seq->readers new-idx-seq# data-reader#)]
               (make-sparse-reader indexes# data#
                                   (quot n-elems# (int new-stride#))
                                   :sparse-value zero-val#))))
         (stride [item#] 1)
         (readers [item#]
           {:indexes index-reader#
            :data data-reader#})
         (iterables [item#]
           (sparse-proto/readers item#))
         sparse-proto/PToSparseReader
         (->sparse-reader [item#] item#)
         dtype-proto/PBufferType
         (buffer-type [item#] :sparse)
         dtype-proto/PBuffer
         (sub-buffer [item# offset# length#]
           (when-not (<= (+ offset# length#)
                         n-elems#)
             (throw (ex-info (format "Sub-buffer out of range: %s > %s"
                                     (+ offset# length#)
                                     n-elems#)
                             {})))
           (let [start-elem# (int (second (dtype-search/binary-search
                                           index-reader# offset# {})))
                 end-elem# (int (second (dtype-search/binary-search
                                         index-reader# (+ offset# length#) {})))
                 sub-len# (- end-elem# start-elem#)
                 new-idx-reader# (->> (dtype-proto/sub-buffer
                                       index-reader# start-elem# sub-len#)
                                      (unary-op/unary-reader-map
                                       {:datatype :int32}
                                       (unary-op/make-unary-op
                                        :int32 (- ~'arg offset#))))
                 new-data-reader# (dtype-proto/sub-buffer
                                   data-reader# start-elem# sub-len#)]
             (make-sparse-reader new-idx-reader# new-data-reader# length#
                                 :sparse-value zero-val#)))))))


(defmacro make-indexed-reader-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-indexed-data-reader ~dtype)])]
        (into {})))


(def indexed-reader-table (make-indexed-reader-table))


(defn ->reader
  [item & [datatype]]
  (let [dtype (or datatype (safe-get-datatype item))]
    (case (argtypes/arg->arg-type item)
      :scalar
      (const-sparse-reader item dtype)
      :iterable
      (dtype-proto/make-container :list
                                  (or datatype
                                      dtype)
                                  item {})
      item)))


(defn make-sparse-reader
  "A sparse reader has no stride or offset but satisfies all of the necessary protocols.
  It does not satisfy the writer or mutable protocols."
  [index-reader data-reader n-elems & {:keys [datatype
                                              sparse-value]}]
  (let [datatype (casting/safe-flatten
                  (or datatype (dtype-base/get-datatype data-reader)))
        create-fn (get indexed-reader-table datatype)
        sparse-value (or sparse-value (make-sparse-value datatype))]
    (create-fn (->reader index-reader datatype)
               (->reader data-reader datatype)
               n-elems
               sparse-value)))


(defn const-sparse-reader
  [item-value & [datatype n-elems]]
  (let [datatype (or datatype (dtype-base/get-value item-value))
        n-elems (int (or n-elems Integer/MAX_VALUE))
        value (casting/cast item-value datatype)]
    (make-sparse-reader [] [] n-elems
                        :datatype datatype
                        :sparse-value value)))
