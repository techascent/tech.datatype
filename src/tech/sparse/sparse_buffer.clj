(ns tech.sparse.sparse-buffer
  (:require [tech.sparse.sparse-base :as sparse-base]
            [tech.sparse.protocols :as sparse-proto]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.casting :as casting]
            [tech.datatype.typecast :as typecast]
            [tech.datatype.unary-op :as unary-op]
            [tech.datatype.binary-search :as dtype-search]
            [tech.datatype.argsort :as argsort]
            [tech.datatype.reader :as reader]
            [clojure.core.matrix.protocols :as mp]))


(declare make-sparse-buffer)


(defmacro make-sparse-writer
  [datatype]
  `(fn [item# dtype# unchecked?#]
     (let [b-offset# (long (:b-offset item#))
           b-stride# (long (:b-stride item#))
           b-elem-count# (long (:b-elem-count item#))
           sparse-value# (casting/datatype->cast-fn :unknown ~datatype (:sparse-value item#))
           indexes# (:indexes item#)
           data# (:data item#)
           index-mutable# (typecast/datatype->mutable :int32 indexes#)
           data-mutable# (typecast/datatype->mutable ~datatype data# unchecked?#)
           data-writer# (typecast/datatype->writer ~datatype data# unchecked?#)
           src-dtype# (dtype-base/get-datatype item#)
           n-elems# (dtype-base/ecount item#)]
       (-> (reify
             ~(typecast/datatype->writer-type datatype)
             (getDatatype [writer#] src-dtype#)
             (size [writer#] n-elems#)
             (write [writer# idx# value#]
               (let [[found?# insert-pos#] (second (dtype-search/binary-search
                                                    indexes# idx# {:datatype :int32}))
                     insert-pos# (int insert-pos#)]
                 (if (= sparse-value# value#)
                   (when found?#
                     (do
                       (.remove index-mutable# insert-pos#)
                       (.remove data-mutable# insert-pos#)))
                   (if found?#
                     (.write data-writer# insert-pos# value#)
                     (do
                       (.insert index-mutable# insert-pos# idx#)
                       (.insert index-mutable# insert-pos# value#))))))
             (invoke [writer# idx# value#]
               (.write writer# (int idx#) (casting/datatype->cast-fn
                                           :unknown ~datatype value#)))
             dtype-proto/PSetConstant
             (set-constant! [writer# offset# value# elem-count#]
               (dtype-proto/set-constant! item# offset# value# elem-count#)))
           (dtype-proto/->writer-of-type dtype# unchecked?#)))))


(defmacro make-sparse-writer-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-sparse-writer ~dtype)])]
        (into {})))


(def sparse-writer-table (make-sparse-writer-table))


(defn- make-base-reader
  [{:keys [b-offset b-elem-count sparse-value
           indexes data] :as sparse-buf}]
  (let [b-offset (long b-offset)
        b-elem-count (long b-elem-count)]
    (-> (sparse-base/make-sparse-reader indexes data
                                        b-elem-count
                                        :sparse-value sparse-value
                                        :datatype (dtype-base/get-datatype sparse-buf))
        (dtype-proto/sub-buffer b-offset b-elem-count))))


(defmacro make-sparse-merge
  [datatype]
  `(fn [sparse-value# lhs-indexes# lhs-data# rhs-indexes# rhs-data# unchecked?#]
     (let [sparse-value# (casting/datatype->cast-fn :unknown ~datatype sparse-value#)
           lhs-iter# (typecast/datatype->iter :int32 lhs-indexes# unchecked?#)
           rhs-iter# (typecast/datatype->iter :int32 rhs-indexes# unchecked?#)
           lhs-data# (typecast/datatype->iter ~datatype lhs-data# unchecked?#)
           rhs-data# (typecast/datatype->iter ~datatype rhs-data# unchecked?#)
           result-indexes# (dtype-proto/make-container :list :int32 0)
           result-data# (dtype-proto/make-container :list ~datatype unchecked?#)
           result-idx-mut# (typecast/datatype->mutable :int32 result-indexes# true)
           result-data-mut# (typecast/datatype->mutable ~datatype result-data# unchecked?#)]
       (loop [left-has-more?# (.hasNext lhs-iter#)
              right-has-more?# (.hasNext rhs-iter#)]
         (when (or left-has-more?# right-has-more?#)
           (cond
             (and left-has-more?# right-has-more?#)
             (let [left-idx# (.current lhs-iter#)
                   right-idx# (.current rhs-iter#)]
               (cond
                 (< left-idx# right-idx#)
                 (let [left-idx# (.nextInt lhs-iter#)
                       left-val# (typecast/datatype->iter-next-fn ~datatype lhs-data#)]
                   (when-not (= sparse-value# left-val#)
                     (.append result-idx-mut# left-idx#)
                     (.append result-data-mut# left-val#)))
                 (= left-idx# right-idx#)
                 (let [left-idx# (.nextInt lhs-iter#)
                       left-val# (typecast/datatype->iter-next-fn ~datatype lhs-data#)
                       right-idx# (.nextInt rhs-iter#)
                       right-val# (typecast/datatype->iter-next-fn ~datatype rhs-data#)]
                   (when-not (= sparse-value# right-val#)
                     (.append result-idx-mut# right-idx#)
                     (.append result-data-mut# right-val#)))
                 :else
                 (let [right-idx# (.nextInt rhs-iter#)
                       right-val# (typecast/datatype->iter-next-fn ~datatype rhs-data#)]
                   (when-not (= sparse-value# right-val#)
                     (.append result-idx-mut# right-idx#)
                     (.append result-data-mut# right-val#)))))
             left-has-more?#
             (while (.hasNext lhs-iter#)
               (let [left-idx# (.nextInt lhs-iter#)
                     left-val# (typecast/datatype->iter-next-fn ~datatype lhs-data#)]
                 (when-not (= sparse-value# left-val#)
                   (.append result-idx-mut# left-idx#)
                   (.append result-data-mut# left-val#))))
             :else
             (while (.hasNext rhs-iter#)
               (let [right-idx# (.nextInt rhs-iter#)
                     right-val# (typecast/datatype->iter-next-fn ~datatype rhs-data#)]
                 (when-not (= sparse-value# right-val#)
                   (.append result-idx-mut# right-idx#)
                   (.append result-data-mut# right-val#)))))
           (recur (.hasNext lhs-iter#)
                  (.hasNext rhs-iter#))))
       {:indexes result-indexes#
        :data result-data#})))


(defmacro make-sparse-merge-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-sparse-merge ~dtype)])]
        (into {})))


(def sparse-merge-table (make-sparse-merge-table))


(defn- global->local
  ^long [^long index ^long b-offset ^long b-stride]
  (-> (* index b-stride)
      (+ b-offset)))


(defn- local->global
  ^long [^long index ^long b-offset ^long b-stride]
  (-> (+ index b-offset)
      (quot b-stride)))



(defrecord SparseBuffer [^long b-offset
                         ^long b-stride
                         ^long b-elem-count
                         sparse-value
                         indexes
                         data]
  mp/PElementCount
  (element-count [item] (quot b-elem-count b-stride))
  dtype-proto/PDatatype
  (get-datatype [item] (dtype-base/get-datatype data))

  dtype-proto/PBuffer
  (sub-buffer [buffer offset length]
    (let [offset (long offset)
          length (long length)
          new-ecount (+ offset length)
          old-ecount (dtype-base/ecount buffer)]
      (when-not (>= old-ecount new-ecount)
        (throw (ex-info (format "Requested length: %s greater than existing: %s"
                                new-ecount old-ecount)
                        {})))
      (->SparseBuffer (+ b-offset offset)
                      b-stride
                      length
                      sparse-value
                      indexes
                      data)))
  (alias? [lhs-buffer rhs-buffer]
    (when (instance? SparseBuffer rhs-buffer)
      (and (dtype-proto/alias? indexes (:indexes rhs-buffer))
           (dtype-proto/alias? data (:data rhs-buffer))
           (= b-offset (long (:b-offset rhs-buffer)))
           (= b-stride (long (:b-stride rhs-buffer)))
           (= b-elem-count (long (:b-elem-count rhs-buffer))))))

  (partially-alias? [lhs-buffer rhs-buffer]
    (when (instance? SparseBuffer rhs-buffer)
      (or (dtype-proto/partially-alias? indexes (:indexes rhs-buffer))
          (dtype-proto/partially-alias? data (:data rhs-buffer)))))

  dtype-proto/PCopyRawData
  (copy-raw->item! [item dest dest-offset options]
    (dtype-base/raw-dtype-copy! item dest dest-offset options))

  dtype-proto/PPrototype
  (from-prototype [item datatype shape]
    (make-sparse-buffer datatype (dtype-base/shape->ecount shape)))


  dtype-proto/PToArray
  (->sub-array [item] nil)
  (->array-copy [item]
    (dtype-proto/->array-copy (sparse-proto/->sparse-reader item)))


  dtype-proto/PToReader
  (->reader-of-type [item datatype unchecked?]
    (-> (sparse-proto/->sparse-reader item)
        (dtype-proto/->reader-of-type datatype unchecked?)))


  dtype-proto/PToIterable
  (->iterable-of-type [item datatype unchecked?]
    (dtype-proto/->reader-of-type item datatype unchecked?))


  dtype-proto/PToWriter
  (->writer-of-type [item datatype unchecked?]
    (let [base-dtype (casting/safe-flatten (dtype-base/get-datatype item))
          writer-fn (get sparse-writer-table base-dtype)]
      (writer-fn item datatype unchecked?)))


  dtype-proto/PWriteIndexes
  (write-indexes! [item new-indexes new-values options]
    (let [n-elems (dtype-base/ecount new-indexes)]
      (when-not (= n-elems 0)
        (let [{new-indexes :new-indexes
               new-values :data} (sparse-base/unordered-global-space->ordered-local-space
                                  new-indexes new-values b-offset b-stride
                                  (:indexes-in-order? options))
              idx-reader (typecast/datatype->reader :int32 new-indexes)
              start-idx (.read idx-reader 0)
              end-idx (.read idx-reader (- n-elems 1))
              offset (long (second (dtype-search/binary-search indexes start-idx :datatype :int32)))
              [found? end-offset] (second (dtype-search/binary-search indexes end-idx :datatype :int32))
              length (long (if found?
                             (+ (long end-offset) 1)
                             end-offset))
              sub-indexes (dtype-proto/sub-buffer indexes offset length)
              sub-data (dtype-proto/sub-buffer data offset length)
              merge-fn (get sparse-merge-table (casting/host-flatten (dtype-base/get-datatype item)))
              {union-indexes :indexes
               union-data :data}
              (merge-fn sparse-value sub-indexes sub-data
                        new-indexes new-values (:unchecked? options))]
          (dtype-base/remove-range! indexes offset length)
          (dtype-base/remove-range! data offset length)
          (dtype-base/insert-block! indexes offset union-indexes {:unchecked? true})
          (dtype-base/insert-block! data offset union-data {:unchecked? true})))))


  dtype-proto/PSetConstant
  (set-constant! [item offset value length]
    (when-not (<= (+ offset length)
                  (dtype-base/ecount item))
      (throw (ex-info (format "Request count (%s) out of range (%s)")
                      (+ offset length)
                      (dtype-base/ecount item))))
    (let [item-dtype (dtype-base/get-datatype item)
          value (casting/cast value item-dtype)
          offset (int offset)
          length (int length)]
      (if (and (= sparse-value value)
               (= b-stride 1))
        (let [start-idx (global->local offset b-offset b-stride)
              end-idx (+ start-idx (max 0 (- length 1)))
              start-pos (int (second (dtype-search/binary-search indexes start-idx {:datatype :int32})))
              end-pos (int (second (dtype-search/binary-search indexes end-idx {:datatype :int32})))
              length (- end-pos start-pos)]
          (dtype-base/remove-range! indexes start-pos length)
          (dtype-base/remove-range! data start-pos length))
        (dtype-proto/write-indexes! item
                                    (reader/reader-range :int32 offset (+ length offset))
                                    (reader/make-const-reader value item-dtype length)
                                    {:unchecked? true
                                     :indexes-in-order? true}))))


  sparse-proto/PToSparseReader
  (->sparse-reader [item]
    (-> (make-base-reader item)
        (sparse-proto/set-stride b-stride)))

  sparse-proto/PSparse
  (index-seq [item]
    (-> (make-base-reader item)
        (sparse-base/get-index-seq b-stride)))

  (sparse-value [item] sparse-value)
  (sparse-ecount [item]
    (let [num-non-sparse
          (long (cond
                  (and (= 0 b-offset)
                       (= 1 b-stride))
                  (dtype-base/ecount indexes)
                  (= 1 b-stride)
                  (dtype-base/ecount (make-base-reader item))
                  :else
                  (count (sparse-proto/index-seq item))))]
      (- b-elem-count num-non-sparse)))

  (set-stride [item new-stride]
    (when-not (= 0 (rem b-elem-count (long new-stride)))
      (throw (ex-info (format "New stride %s is not commensurate with elem-count %s"
                              new-stride b-elem-count)
                      {})))
    (->SparseBuffer b-offset (* b-stride new-stride)
                    b-elem-count
                    sparse-value
                    indexes
                    data))

  (stride [item] b-stride)

  (readers [item]
    (sparse-proto/readers (sparse-proto/->sparse-reader item)))

  (iterables [item]
    (let [base-reader (make-base-reader item)]
      (sparse-base/index-seq->iterables (sparse-base/get-index-seq b-stride base-reader)
                                        (sparse-proto/data-reader base-reader)))))


(defn copy-sparse->any
  "Src *must* be a sparse buffer."
  [src dst options]
  (when-not (and (sparse-proto/is-sparse? src))
    (throw (ex-info "Source item must be sparse" {})))
  (let [n-elems (dtype-base/ecount src)
        {src-indexes :indexes
         src-data :data} (sparse-proto/readers (sparse-proto/->sparse src))]
    (dtype-proto/set-constant! dst 0 (sparse-proto/sparse-value src) n-elems)
    (dtype-proto/write-indexes! dst src-indexes src-data
                                (assoc options :indexes-in-order? true))
    dst))


(defn copy-dense->sparse
  [src dst options]
  (let [n-elems (dtype-base/ecount src)]
    (dtype-proto/write-indexes! dst (reader/reader-range :int32 0 n-elems) src
                                (assoc options :indexes-in-order? true))
    dst))
