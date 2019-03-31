(ns tech.sparse.buffer
  (:require [tech.datatype :as dtype]
            [tech.datatype.casting :as casting]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.binary-search :as dtype-search]
            [tech.datatype.typecast :as typecast]
            [clojure.core.matrix.protocols :as mp]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.sparse.set-ops :as set-ops]
            [tech.sparse.protocols :as sparse-proto]
            [tech.sparse.index-buffer :as sparse-index]
            [tech.sparse.utils :as utils]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro make-sparse-reader
  [datatype]
  `(fn [item# unchecked?#]
     (when-not (= (casting/safe-flatten (dtype-base/get-datatype item#))
                  ~datatype)
       (throw (ex-info (format "Datatype mismatch: %s, %s"
                               ~datatype
                               (casting/safe-flatten (dtype-base/get-datatype item#)))
                       {})))
     (let [n-elems# (dtype-base/ecount item#)
           data-buffer# (sparse-proto/data-buffer item#)
           data-reader# (typecast/datatype->reader ~datatype data-buffer# unchecked?#)
           index-buffer# (sparse-proto/index-buffer item#)]
       (reify ~(typecast/datatype->reader-type datatype)
         (getDatatype [_#] (dtype-base/get-datatype item#))
         (size [_#] n-elems#)
         (read [reader# idx#]
           (let [[found?# item-idx#] (sparse-proto/find-index index-buffer# idx#)]
             (if found?#
               (.read data-reader# (int item-idx#))
               (casting/datatype->sparse-value ~datatype))))
         (invoke [reader# arg#]
           (.read reader# (int arg#)))
         dtype-proto/PBuffer
         (sub-buffer [reader# offset# length#]
           (-> (dtype-proto/sub-buffer item# offset# length#)
               (dtype-proto/->reader-of-type ~datatype unchecked?#)))
         (alias? [reader# rhs#]
           (dtype-proto/alias? item# rhs#))
         (partially-alias? [reader# rhs#]
           (dtype-proto/partially-alias? item# rhs#))
         sparse-proto/PToSparseBuffer
         (->sparse-buffer [reader#] item#)))))


(defmacro make-sparse-reader-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-sparse-reader ~dtype)])]
        (into {})))


(def sparse-reader-table (make-sparse-reader-table))


(defmacro make-sparse-writer
  [datatype]
  `(fn [item# unchecked?#]
     (when-not (= (casting/safe-flatten (dtype-base/get-datatype item#))
                  ~datatype)
       (throw (ex-info (format "Datatype mismatch: %s, %s"
                               ~datatype
                               (casting/safe-flatten (dtype-base/get-datatype item#)))
                       {})))
     (let [n-elems# (dtype-base/ecount item#)
           data-buffer# (sparse-proto/data-buffer item#)
           data-writer# (typecast/datatype->writer ~datatype data-buffer# unchecked?#)
           index-buffer# (sparse-proto/index-buffer item#)
           sparse-value# (casting/datatype->sparse-value ~datatype)]
       (reify ~(typecast/datatype->writer-type datatype)
         (getDatatype [_#] (dtype-base/get-datatype item#))
         (size [_#] n-elems#)
         (write [writer# idx# value#]
           (let [[found?# item-idx#] (sparse-proto/find-index index-buffer# idx#)]
             (if-not (= value# sparse-value#)
               (if found?#
                 (.write data-writer# (int item-idx#) value#)
                 (do
                   (sparse-proto/insert-index! index-buffer# item-idx# idx#)
                   (dtype/insert! data-buffer# item-idx# value#)))
               ;;Else if sparse value remove it from record.
               (when found?#
                 (sparse-proto/remove-index! index-buffer# idx#)
                 (dtype/remove! data-buffer# item-idx#)))))
         (invoke [writer# arg# value#]
           (.write writer# (int arg#) (casting/datatype->cast-fn
                                       :unknown ~datatype value#)))
         dtype-proto/PBuffer
         (sub-buffer [writer# offset# length#]
           (-> (dtype-proto/sub-buffer item# offset# length#)
               (dtype-proto/->writer-of-type ~datatype unchecked?#)))
         (alias? [writer# rhs#]
           (dtype-proto/alias? item# rhs#))
         (partially-alias? [writer# rhs#]
           (dtype-proto/partially-alias? item# rhs#))
         sparse-proto/PToSparseBuffer
         (->sparse-buffer [writer#] item#)))))


(defmacro make-sparse-writer-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-sparse-writer ~dtype)])]
        (into {})))


(def sparse-writer-table (make-sparse-writer-table))


(defrecord SparseBuffer [data-buf
                         index-buf]
  dtype-proto/PDatatype
  (get-datatype [item] (dtype-proto/get-datatype data-buf))

  sparse-proto/PSparse
  (index-seq [item] (sparse-proto/index-seq index-buf))
  (set-stride [item new-stride]
    (->SparseBuffer data-buf (sparse-proto/set-stride index-buf new-stride)
                    (sparse-proto/zero-value item)))
  (stride [item]
    (sparse-proto/stride index-buf))
  (->nio-index-buffer [item]
    (sparse-proto/->nio-index-buffer index-buf))

  sparse-proto/PSparseBuffer
  (set-values! [item new-idx-buf new-data-buf]
    (sparse-proto/set-index-values! index-buf data-buf new-idx-buf
                                    new-data-buf
                                    (sparse-proto/zero-value item)))

  (set-sequential-values! [item data-buffer]
    (sparse-proto/set-values! item
                              (dtype-proto/make-container :list
                               :int32 (range (dtype/ecount data-buffer)))
                              data-buffer))
  (->sparse-buffer-backing-store [item] item)
  (data-buffer [item]
    (let [start-idx (long (sparse-proto/find-index index-buf 0))
          end-idx (long (sparse-proto/find-index index-buf (dtype/ecount index-buf)))]
      (dtype-proto/sub-buffer data-buf start-idx (- end-idx start-idx))))
  (->nio-data-buffer [item]
    (when (= 1 (int (sparse-proto/stride index-buf)))
      (let [start-idx (int (second (sparse-proto/find-index index-buf 0)))
            end-idx (int (second (sparse-proto/find-index index-buf (dtype/ecount index-buf))))]
        (-> (dtype-proto/sub-buffer data-buf
                                    start-idx
                                    (- end-idx start-idx))
            (dtype-proto/->buffer-backing-store)))))
  (index-buffer [item] index-buf)
  (zero-value [item] (case (casting/host-flatten (dtype-base/get-datatype data-buf))
                       :int8 (casting/datatype->sparse-value :int8)
                       :int16 (casting/datatype->sparse-value :int16)
                       :int32 (casting/datatype->sparse-value :int32)
                       :int64 (casting/datatype->sparse-value :int64)
                       :float32 (casting/datatype->sparse-value :float32)
                       :float64 (casting/datatype->sparse-value :float64)
                       :boolean (casting/datatype->sparse-value :boolean)
                       :object (casting/datatype->sparse-value :object)))

  dtype-proto/PBuffer
  (sub-buffer [buffer offset length]
    (->SparseBuffer data-buf
                    (dtype-proto/sub-buffer index-buf offset length)
                    (sparse-proto/zero-value buffer)))
  (alias? [lhs-dev-buffer rhs-dev-buffer]
    (and (instance? SparseBuffer rhs-dev-buffer)
         (dtype-proto/alias? (:data-buf lhs-dev-buffer)
                             (:data-buf rhs-dev-buffer))
         (dtype-proto/alias? (:index-buf lhs-dev-buffer)
                             (:index-buf rhs-dev-buffer))
         (= (:zero-val lhs-dev-buffer)
            (:zero-val rhs-dev-buffer))))
  (partially-alias? [lhs-dev-buffer rhs-dev-buffer]
    (and (instance? SparseBuffer rhs-dev-buffer)
         (dtype-proto/partially-alias? (:data-buf lhs-dev-buffer)
                                       (:data-buf rhs-dev-buffer))
         (dtype-proto/partially-alias? (:index-buf lhs-dev-buffer)
                                       (:index-buf rhs-dev-buffer))))

  sparse-proto/PToSparseBuffer
  (->sparse-buffer [item] item)

  mp/PElementCount
  (element-count [item] (mp/element-count index-buf))

  dtype-proto/PToArray
  (dtype-proto/->sub-array [item] nil)
  (dtype-proto/->array-copy [item]
    (let [n-elems (mp/element-count item)
          retval (dtype-proto/make-container :java-array
                                             (casting/safe-flatten
                                              (dtype-proto/get-datatype item))
                                             n-elems)]
      (dtype/copy! item 0 retval 0 n-elems {:unchecked? true})
      retval))

  dtype-proto/PToReader
  (->reader-of-type [item datatype unchecked?]
    (let [reader-fn (get sparse-reader-table (casting/safe-flatten
                                              (dtype-base/get-datatype item)))]
      (-> (reader-fn item true)
          (dtype-proto/->reader-of-type datatype unchecked?))))


  dtype-proto/PToWriter
  (->writer-of-type [item datatype unchecked?]
    (let [writer-fn (get sparse-writer-table (casting/safe-flatten
                                              (dtype-base/get-datatype item)))]
      (-> (writer-fn item true)
          (dtype-proto/->writer-of-type datatype unchecked?)))))


(defn make-sparse-buffer
  ([datatype elem-count-or-seq options]
   (cond
     (number? elem-count-or-seq)
     (->SparseBuffer (dtype-proto/make-container :list datatype 0)
                     (sparse-index/make-index-buffer elem-count-or-seq [])
                     (dtype/cast 0 datatype))
     :else
     (let [buf-size (count elem-count-or-seq)
           zero-val (dtype/cast 0 datatype)
           nonzero-pairs (->> elem-count-or-seq
                              (map-indexed (fn [idx data-val]
                                             (when-not
                                                 (= zero-val
                                                    (dtype/cast data-val datatype))
                                               [idx data-val])))
                              (remove nil?))]
       (->SparseBuffer (dtype-proto/make-container :list datatype
                                                   (map second nonzero-pairs))
                       (sparse-index/make-index-buffer buf-size
                                                       (map first nonzero-pairs))
                       zero-val))))
  ([datatype elem-count-or-seq]
   (make-sparse-buffer datatype elem-count-or-seq {})))


(defn copy-sparse->dense-unchecked!
  "Call at your own risk.  The types have to match, more or less."
  [src src-offset dest dest-offset n-elems options]
  (let [src (-> (sparse-proto/->sparse-buffer-backing-store src)
                (dtype-proto/sub-buffer src-offset n-elems))
        dest (dtype-proto/sub-buffer dest dest-offset n-elems)]
    ;;Start with the obvious
    (dtype/set-constant! dest 0 (sparse-proto/zero-value src) n-elems)
    (utils/typed-sparse-copy! (sparse-proto/data-buffer src)
                              (dtype-proto/->buffer-backing-store dest)
                              (sparse-proto/index-seq src))
    dest))


(defn copy-dense->sparse-unchecked!
  [src src-offset dest dest-offset n-elems options]
  (let [src (dtype-proto/sub-buffer src src-offset n-elems)
        dest (-> (sparse-proto/->sparse-buffer-backing-store dest)
                 (dtype-proto/sub-buffer dest-offset n-elems))]
    ;;Not the most efficient way but it will work.
    (dtype/set-constant! dest 0 (sparse-proto/zero-value dest) n-elems)
    (sparse-proto/set-sequential-values! dest src)
    dest))


(defn- generic-sparse->sparse!
  [src dest unchecked?]
  (let [target-indexes (dtype-proto/make-container :list :int32 0 {})
        index-mutable (typecast/datatype->mutable :int32 target-indexes true)
        dest-dtype (dtype/get-datatype dest)
        target-data (dtype-proto/make-container :list dest-dtype 0 {})
        target-mutable (typecast/datatype->mutable :object target-data unchecked?)
        src-data (typecast/datatype->reader :object (sparse-proto/data-buffer src))]
    ;;Can't think of a faster way at this moment.
    (doseq [{:keys [data-index global-index]} (sparse-proto/index-seq src)]
      (.append index-mutable (int global-index))
      (.append target-mutable (.read src-data data-index)))
    (sparse-proto/set-values! dest target-indexes target-data)))


(defn copy-sparse->sparse!
  [src src-offset dest dest-offset n-elems {:keys [unchecked?] :as options}]
  (let [src (-> (sparse-proto/->sparse-buffer-backing-store src)
                (dtype-proto/sub-buffer src-offset n-elems))
        old-dest dest
        dest (-> (sparse-proto/->sparse-buffer-backing-store dest)
                 (dtype-proto/sub-buffer dest-offset n-elems))]
    (dtype/set-constant! dest 0 (sparse-proto/zero-value src) n-elems)
    (if-let [src-nio-indexes (sparse-proto/->nio-index-buffer src)]
      (sparse-proto/set-values! dest
                                src-nio-indexes
                                (sparse-proto/->nio-data-buffer src))
      (generic-sparse->sparse! src dest unchecked?))))
