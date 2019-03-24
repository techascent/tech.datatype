(ns tech.sparse.utils
  (:require [tech.datatype.typecast :as typecast]
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


(defn get-index-seq
  [index-buf data-offset data-stride]
  (let [idx-reader (typecast/datatype->reader :int32 index-buf true)
        data-offset (int data-offset)
        data-stride (int data-stride)]
    (if (or (not= 1 (int data-stride))
            (not= 0 data-offset))
      (->> idx-reader
           (map-indexed
            (fn [data-idx set-idx]
              (let [set-idx (int set-idx)]
                (when (>= set-idx data-offset)
                  (let [adjusted-idx (- (int set-idx) data-offset)]
                    (when (= 0 (rem adjusted-idx data-stride))
                      [data-idx (quot adjusted-idx data-stride)]))))))
           (remove nil?))
      idx-reader)))


(defprotocol PSparseCopier
  (sparse-write [item lhs rhs]))


(defmacro ^:private do-make-sparse-copier
  [datatype lhs-buffer rhs-buffer]
  `(let [lhs-buffer# (typecast/datatype->reader ~datatype ~lhs-buffer)
         rhs-buffer# (typecast/datatype->writer ~datatype ~rhs-buffer)]
     (reify PSparseCopier
       (sparse-write [item lhs-idx# rhs-idx#]
         (.put rhs-buffer# (+ rhs-pos# (unchecked-int rhs-idx#))
               (.get lhs-buffer# (+ lhs-pos# (unchecked-int lhs-idx#))))))))


(defn make-sparse-copier
  [lhs-buf rhs-buf]
  (let [lhs-root (primitive/->buffer-backing-store lhs-buf)
        rhs-root (primitive/->buffer-backing-store rhs-buf)
        lhs-dtype (dtype/get-datatype lhs-root)]
    (when-not (= lhs-dtype (dtype/get-datatype rhs-root))
      (throw (ex-info "Copiers not available unless datatypes match." {})))
    (case lhs-dtype
      :int8 (do-make-sparse-copier :int8 lhs-root rhs-root)
      :int16 (do-make-sparse-copier :int16 lhs-root rhs-root)
      :int32 (do-make-sparse-copier :int32 lhs-root rhs-root)
      :int64 (do-make-sparse-copier :int64 lhs-root rhs-root)
      :float32 (do-make-sparse-copier :float32 lhs-root rhs-root)
      :float64 (do-make-sparse-copier :float64 lhs-root rhs-root))))


(defn- do-typed-sparse-copy!
  [copier index-seq]
  (doseq [[lhs-idx rhs-idx] index-seq]
    (sparse-write copier lhs-idx rhs-idx)))

(defn typed-sparse-copy!
  [src dest index-seq]
  (do-typed-sparse-copy! (make-sparse-copier src dest) index-seq))
