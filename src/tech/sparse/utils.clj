(ns tech.sparse.utils
  (:require [tech.datatype.typecast :as typecast]
            [tech.datatype.casting :as casting]
            [tech.datatype :as dtype]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn binary-search
  "Perform binary search returning long idx of matching value or insert position.
  Returns index of the element or the index where it should be inserted.  Index buf is
  expected to be convertible to a int32 nio-buffer."
  ^long [idx-buf ^long target]
  (let [idx-buf (typecast/datatype->reader :int32 idx-buf true)
        target (int target)
        buf-ecount (dtype/ecount idx-buf)]
    (if (= 0 buf-ecount)
      0
      (loop [low (long 0)
             high (long buf-ecount)]
        (if (< low high)
          (let [mid (+ low (quot (- high low) 2))
                buf-data (.read idx-buf mid)]
            (if (= buf-data target)
              (recur mid mid)
              (if (and (< buf-data target)
                       (not= mid low))
                (recur mid high)
                (recur low mid))))
          (let [buf-val (.read idx-buf low)]
            (if (<= target buf-val)
              low
              (unchecked-inc low))))))))


(defrecord IndexSeqRec [^long data-index ^long global-index])


(defn get-index-seq
  "Given the offset and stride of the buffer
  return an index sequence that contains a sequence of
  tuples that contain"
  [data-offset data-stride index-buf]
  (let [data-offset (int data-offset)
        data-stride (int data-stride)
        [first-idx index-buf]
        (if (= 0 data-offset)
          [0 index-buf]
          (let [off-idx (binary-search index-buf data-offset)]
            [off-idx (dtype/sub-buffer index-buf off-idx)]))
        first-idx (int first-idx)
        index-seq (->> index-buf
                       (typecast/datatype->reader :int32)
                       (map ->IndexSeqRec
                            (range first-idx)))]
    (if (= 1 data-stride)
      index-seq
      (map (fn [record]
             (let [global-index (int (:global-index ^IndexSeqRec record))]
               (when (= 0 (rem global-index data-stride))
                 (assoc record :global-index (quot global-index data-stride)))))))))



(defmacro ^:private make-sparse-copier
  [datatype]
  `(fn [lhs-buffer# rhs-buffer# index-seq#]
     (let [lhs-buffer# (typecast/datatype->reader ~datatype lhs-buffer#)
           rhs-buffer# (typecast/datatype->writer ~datatype rhs-buffer#)]
       (doseq [{:keys [data-index# global-index#]} index-seq#]
         (.write rhs-buffer# (int global-index#)
                 (.read lhs-buffer# (int data-index#)))))))


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
