(ns tech.v2.datatype.sparse.reader
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.reader :as reader]
            [tech.v2.datatype.readers.indexed :as indexed-reader]
            [tech.v2.datatype.iterator :as iterator]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.sparse.protocols :as sparse-proto]
            [tech.v2.datatype.binary-search :as dtype-search]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.argtypes :as argtypes]))


(declare ->reader const-sparse-reader make-sparse-reader)


(defrecord IndexSeqRec [^long data-index ^long global-index])


(defn get-index-seq
  "Create an index-seq out of an int32 reader"
  [index-iterable]
  (->> (dtype-proto/->iterable index-iterable {:datatype :int32})
       (map-indexed #(->IndexSeqRec %1 %2))))


(defn index-seq->readers
  [index-seq & [data-reader]]
  (merge {:indexes (->reader (map :global-index index-seq) :int32)}
         (when data-reader
           {:data (indexed-reader/make-indexed-reader
                   (->reader (map :data-index index-seq) :int32)
                   (->reader data-reader)
                   {})})))


(defn index-seq->iterables
  [index-seq & [data-reader]]
  (merge {:indexes (map :global-index index-seq)}
         (when data-reader
           {:data (indexed-reader/make-iterable-indexed-iterable
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
  `(fn [index-reader# data-reader# n-elems# zero-val# datatype#]
     (let [local-data-reader# (typecast/datatype->reader ~datatype data-reader# true)
           index-reader# (typecast/datatype->reader :int32 index-reader# true)
           n-elems# (long n-elems#)
           idx-count# (dtype-base/ecount index-reader#)
           zero-val# (casting/datatype->cast-fn :unknown ~datatype zero-val#)]
       (reify
         ~(typecast/datatype->reader-type datatype)
         (getDatatype [reader#] datatype#)
         (lsize [reader#] n-elems#)
         (read [reader# idx#]
           (let [[found?# data-idx#] (dtype-search/binary-search
                                      index-reader# (int idx#)
                                      {:datatype :int32})]
             (if found?#
               (.read local-data-reader# (int data-idx#))
               zero-val#)))
         dtype-proto/PToBackingStore
         (->backing-store-seq [item#]
           (concat (dtype-proto/->backing-store-seq index-reader#)
                   (dtype-proto/->backing-store-seq data-reader#)))
         sparse-proto/PSparse
         (index-seq [reader#]
           (get-index-seq index-reader#))
         (sparse-value [reader#] zero-val#)
         (sparse-ecount [reader#] (- n-elems# idx-count#))
         (readers [item#]
           {:indexes index-reader#
            :data data-reader#})
         (iterables [item#]
           (sparse-proto/readers item#))
         sparse-proto/PToSparse
         (convertible-to-sparse? [item#] true)
         (->sparse [item#] item#)
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
                                           index-reader# offset#
                                           {:datatype :int32})))
                 end-elem# (int (second (dtype-search/binary-search
                                         index-reader# (+ offset# length#)
                                         {:datatype :int32})))
                 sub-len# (- end-elem# start-elem#)
                 new-idx-reader# (->> (dtype-proto/sub-buffer
                                       index-reader# start-elem# sub-len#)
                                      (unary-op/unary-reader-map
                                       {:datatype :int32}
                                       (unary-op/make-unary-op
                                        :int32 (- ~'x offset#))))
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
  (let [dtype (or datatype (dtype-proto/get-datatype item))]
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
  "A sparse reader has no offset but satisfies all of the necessary protocols.
  It does not satisfy the writer or mutable protocols."
  [index-reader data-reader n-elems & {:keys [datatype
                                              sparse-value]}]
  (let [datatype (casting/safe-flatten
                  (or datatype (dtype-proto/get-datatype data-reader)))
        create-fn (get indexed-reader-table datatype)
        sparse-value (or sparse-value (make-sparse-value datatype))]

    (create-fn index-reader data-reader
               n-elems
               sparse-value
               datatype)))


(defn const-sparse-reader
  [item-value & [datatype n-elems]]
  (let [datatype (or datatype (dtype-base/get-datatype item-value))
        n-elems (int (or n-elems Integer/MAX_VALUE))
        value (casting/cast item-value datatype)]
    (make-sparse-reader [] [] n-elems
                        :datatype datatype
                        :sparse-value value)))
