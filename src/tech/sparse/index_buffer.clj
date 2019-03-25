(ns tech.sparse.index-buffer
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.typecast :as typecast]
            [tech.datatype.reader :as reader]
            [tech.sparse.protocols :as sparse-proto]
            [clojure.core.matrix.protocols :as mp]
            [tech.sparse.utils :as utils]
            [tech.datatype :as dtype]
            [tech.sparse.set-ops :as set-ops]
            [clojure.core.matrix.macros :refer [c-for]]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare find-index)


(defn- relative-offset
  ^long [^long buf-offset
         ^long buf-stride
         ^long offset]
  (-> (* offset buf-stride)
      (+ buf-offset)))


(defn- relative-elem-count
  ^long [^long buf-stride ^long n-elems]
  (* n-elems buf-stride))

(defn find-index
  ^long [index-buffer item-value]
  (long (sparse-proto/find-index index-buffer item-value)))

(defn stride
  ^long [index-buffer]
  (long (sparse-proto/stride index-buffer)))


;; Index buffer that allows offsetting and striding but still shares the backing store
;; with other buffers.  Index buffers are sorted lists of integers.  They have the same
;; length as data buffers but the integers indicate which index the data buffer's
;; associated data lies at.
(defrecord IndexBuffer [index-data
                        ^long b-offset ;;In elements, not strided elements
                        ^long b-stride ;;Stride of the buffer
                        ^long b-elem-count ;;in actual elements
                        ]
  dtype-proto/PDatatype
  (get-datatype [item] (dtype-base/get-datatype index-data))


  mp/PElementCount
  (element-count [item] (quot (+ b-elem-count
                                 (- b-stride 1))
                              b-stride))

  sparse-proto/PSparse
  (index-seq [item]
    (let [start-idx (find-index item 0)
          end-target-idx (dtype/ecount item)
          end-idx (find-index item end-target-idx)
          n-idx-elems (- end-idx start-idx)]
      (utils/get-index-seq
       (dtype-proto/sub-buffer index-data
                               start-idx
                               n-idx-elems)
       b-offset
       b-stride)))
  (set-stride [item new-stride]
    (let [new-stride (long new-stride)]
      (when-not (= 0 (rem (dtype/ecount item) new-stride))
        (throw (ex-info "Element count and stride must be commensurate"
                        {:element-count (dtype/ecount item)
                         :new-stride new-stride})))
      (->IndexBuffer index-data
                     b-offset
                     (* b-stride (long new-stride))
                     b-elem-count)))
  (stride [item] b-stride)
  (->nio-index-buffer [item]
    (when (= 1 b-stride)
      (let [start-idx (find-index item 0)
            end-idx (find-index item (dtype/ecount item))]
        (-> (dtype-proto/sub-buffer index-data start-idx (- end-idx start-idx))
            (dtype-proto/->buffer-backing-store)))))


  sparse-proto/PSparseIndexBuffer
  ;;Returns an integer that may point to either the index or where
  ;;the item should be inserted.
  (find-index [idx-buf target-idx]
    (utils/binary-search index-data
                         (relative-offset b-offset
                                          b-stride
                                          target-idx)))

  (found-index? [idx-buf buf-idx target-idx]
    (let [buf-idx (int buf-idx)
          target-idx (relative-offset b-offset b-stride target-idx)]
      (and (not= buf-idx (dtype/ecount index-data))
           (= (reader/typed-read :int32 index-data buf-idx)
              target-idx))))

  (set-index-values! [item old-data-buf new-idx-buf new-data-buf zero-val]
    (when-not (= (dtype/ecount new-data-buf)
                 (dtype/ecount new-idx-buf))
      (throw (ex-info "index, data buffer count mismatch"
                      {:index-buffer-ecount (dtype/ecount new-idx-buf)
                       :data-buffer-ecount (dtype/ecount new-data-buf)})))
    (when (not= 0 (dtype/ecount new-idx-buf))
      (let [buf-dtype (dtype/get-datatype old-data-buf)
            _ (when-not (= buf-dtype (dtype/get-datatype new-data-buf))
                (throw (ex-info "Datatypes do not match"
                                {:old-datatype buf-dtype
                                 :new-datatype (dtype/get-datatype new-data-buf)})))

            ;;Transform the index buffer into our index buffer's base element space
            new-idx-buf (if (or (not= 1 b-stride)
                                (not= 0 b-offset))
                          (ct/binary-op! (ct/from-prototype new-idx-buf)
                                         b-stride new-idx-buf
                                         1.0 b-offset :+)
                          new-idx-buf)

            first-new-idx (int (dtype/get-value new-idx-buf 0))
            last-new-idx (int (dtype/get-value new-idx-buf
                                               (- (dtype/ecount new-idx-buf) 1)))

            first-old-idx (utils/binary-search index-data first-new-idx)
            last-old-idx (utils/binary-search index-data (unchecked-inc last-new-idx))
            remove-count (- last-old-idx first-old-idx)
            _ (when (< remove-count 0)
                (throw (ex-info "Index buf logic error"
                                {:first-new-idx first-new-idx
                                 :last-new-idx last-new-idx
                                 :first-old-idx first-old-idx
                                 :last-old-idx last-old-idx})))
            {union-indexes :indexes
             union-values :values} (if (not= 0 remove-count)
                                     (set-ops/union-values
                                      (dtype-proto/sub-buffer index-data
                                                              first-old-idx
                                                              remove-count)
                                      (dtype-proto/sub-buffer old-data-buf
                                                              first-old-idx
                                                              remove-count)
                                      new-idx-buf new-data-buf
                                      zero-val)
                                     {:indexes new-idx-buf
                                      :values new-data-buf})]
        (when-not (= 0 remove-count)
          (fu-dtype/remove-range! index-data first-old-idx remove-count)
          (fu-dtype/remove-range! old-data-buf first-old-idx remove-count))
        (fu-dtype/insert-elems! index-data first-old-idx union-indexes)
        (fu-dtype/insert-elems! old-data-buf first-old-idx union-values)))
    item)
  (insert-index! [item data-idx idx]
    (let [real-idx (relative-offset b-offset b-stride idx)]
      (fu-dtype/insert! index-data data-idx real-idx)))
  (remove-index! [item idx]
    (let [data-idx (find-index item idx)]
      (when (sparse-proto/found-index? item data-idx idx)
        (fu-dtype/remove-range! index-data data-idx 1)
        data-idx)))
  (remove-sequential-indexes! [item data-buffer]
    (let [start-idx (find-index item 0)
          end-idx (find-index item (dtype/ecount item))
          n-elems (- end-idx start-idx)]
      (if (= b-stride 1)
        (do
          (fu-dtype/remove-range! index-data start-idx n-elems)
          (fu-dtype/remove-range! data-buffer start-idx n-elems))
        (loop [idx (int 0)
               n-elems n-elems]
          (when (< idx n-elems)
            (let [local-idx (+ start-idx idx)]
              (if (= 0 (rem (.getInt index-data (+ start-idx idx))
                            b-stride))
                (do
                  (.removeInt index-data local-idx)
                  (fu-dtype/remove-range! data-buffer local-idx 1)
                  (recur idx (unchecked-dec n-elems)))
                (recur (unchecked-inc idx) n-elems)))))))
    item)
  (offset [item] b-offset)

  dtype-proto/PBuffer
  (sub-buffer [_ off len]
    (let [data-off (+ b-offset (* (int off) b-stride))
          data-len (* (int len) b-stride)]
      (when (> (+ data-off data-len)
               (+ b-offset b-elem-count))
        (throw (ex-info "Sub buffer out of range"
                        {:desired-offset off
                         :desired-length len
                         :data-off data-off
                         :data-len data-len
                         :b-elem-count b-elem-count
                         :ecount (dtype/ecount _)})))
      (->IndexBuffer index-data
                     data-off
                     b-stride
                     data-len)))
  (alias? [lhs rhs]
    (and (instance? IndexBuffer rhs)
         (dtype-proto/alias? (dtype-proto/->buffer-backing-store lhs)
                             (dtype-proto/->buffer-backing-store rhs))
         (= b-stride (int (:b-stride rhs)))
         (= b-offset (int (:b-offset rhs)))
         (= b-elem-count (int (:b-elem-count rhs)))))

  (partially-alias? [lhs rhs]
    (and (instance? IndexBuffer rhs)
         (dtype-proto/partially-alias? (dtype-proto/->buffer-backing-store lhs)
                                       (dtype-proto/->buffer-backing-store rhs)))))


(defn make-index-buffer
  [buf-ecount idx-seq]
  (->IndexBuffer (fu-dtype/make-fastutil-list :int32 idx-seq)
                 (long 0)
                 (long 1)
                 (long buf-ecount)))
