(ns tech.sparse.utils
  (:require [tech.datatype.typecast :as typecast]
            [tech.datatype.casting :as casting]
            [tech.datatype :as dtype]
            [tech.datatype.unary-op :as unary-op]
            [tech.datatype.binary-search :as dtype-search])
  (:import [tech.datatype UnaryOperators$IntUnary]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


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
          (let [off-idx (dtype-search/binary-search index-buf data-offset)]
            [off-idx (dtype/sub-buffer index-buf off-idx)]))
        first-idx (int first-idx)
        index-seq (if (= 0 data-offset)
                    index-buf
                    (unary-op/unary-reader-map
                     (unary-op/make-unary-op :int32 (- arg data-offset))
                     adj-index-buf))
        index-seq (map ->IndexSeqRec
                       (range first-idx n-elems)
                       index-seq)]
    (if (= 1 data-stride)
      index-seq
      (->> index-seq
           (map (fn [record]
                  (let [global-index (int (:global-index record))]
                    (when (= 0 (rem global-index data-stride))
                      (assoc record :global-index
                             (quot global-index data-stride))))))
           (remove nil?)))))



(defmacro ^:private make-sparse-copier
  [datatype]
  `(fn [lhs-buffer# rhs-buffer# index-seq#]
     (let [lhs-buffer# (typecast/datatype->reader ~datatype lhs-buffer#)
           rhs-buffer# (typecast/datatype->writer ~datatype rhs-buffer#)]
       (doseq [{:keys [~'data-index ~'global-index]} index-seq#]
         (.write rhs-buffer# (int ~'global-index)
                 (.read lhs-buffer# (int ~'data-index)))))
     rhs-buffer#))


(defmacro make-sparse-copier-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             [dtype `(make-sparse-copier ~dtype)])]
        (into {})))


(def sparse-copier-table (make-sparse-copier-table))


(defn typed-sparse-copy!
  [src dest index-seq]
  (if-let [copier-fn (get sparse-copier-table
                          (-> (dtype/get-datatype dest)
                              casting/flatten-datatype))]
    (copier-fn src dest index-seq)
    (throw (ex-info (format "Failed to find copier for datatype: %s"
                            (dtype/get-datatype dest))))))
