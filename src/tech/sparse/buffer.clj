(ns tech.sparse.buffer
  (:require [tech.libs.fastutil.datatype :as fu-dtype]
            [tech.datatype :as dtype]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.java-primitive :as primitive]
            [tech.datatype.java-unsigned :as unsigned]
            [clojure.core.matrix.protocols :as mp]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.compute.cpu.typed-buffer :as cpu-typed-buf]
            [tech.compute.driver :as compute-drv]
            [tech.compute.tensor.functional :as tens-func]
            [tech.sparse.set-ops :as set-ops]
            [tech.sparse.protocols :as sparse-proto]
            [tech.sparse.index-buffer :as sparse-index]
            [tech.sparse.utils :refer [binary-search get-index-seq]
             :as utils])
  (:import [it.unimi.dsi.fastutil.ints IntArrayList]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defrecord SparseBuffer [data-buf
                         index-buf
                         zero-val]
  dtype-base/PDatatype
  (get-datatype [item] (dtype-base/get-datatype data-buf))

  dtype-base/PContainerType
  (container-type [item] :sparse-buffer)

  dtype-base/PAccess
  (get-value [item idx]
    (let [buf-idx (sparse-proto/find-index index-buf idx)]
      (if (sparse-proto/found-index? index-buf buf-idx idx)
        (dtype/get-value data-buf buf-idx)
        zero-val)))

  (set-constant! [item offset value elem-count]
    (let [elem-count (int elem-count)
          offset (int offset)
          value (dtype/cast value (dtype/get-datatype data-buf))]
      (if (= value zero-val)
        (sparse-proto/remove-sequential-indexes!
         (compute-drv/sub-buffer index-buf offset elem-count)
         data-buf)
        (let [new-idx-buf (fu-dtype/make-fastutil-list :int32 (range elem-count))
              new-data-buf (fu-dtype/make-fastutil-list
                            (dtype/get-datatype data-buf)
                            (repeat elem-count value))]
          (sparse-proto/set-index-values!
           index-buf data-buf
           new-idx-buf new-data-buf zero-val)))
      item))

  (set-value! [item idx value]
    (let [value (dtype/cast value (dtype/get-datatype item))]
      (if (= zero-val value)
        (when-let [data-idx (sparse-proto/remove-index! index-buf idx)]
          (fu-dtype/remove-range! data-buf data-idx 1))
        (let [data-idx (sparse-proto/find-index index-buf idx)]
          (if (sparse-proto/found-index? index-buf data-idx idx)
            (dtype/set-value! data-buf data-idx value)
            (do
              (fu-dtype/insert! data-buf data-idx value)
              (sparse-proto/insert-index! index-buf data-idx idx)))))))

  sparse-proto/PSparse
  (index-seq [item] (sparse-proto/index-seq index-buf))
  (set-stride [item new-stride]
    (->SparseBuffer data-buf (sparse-proto/set-stride index-buf new-stride) zero-val))
  (stride [item]
    (sparse-proto/stride index-buf))
  (->nio-index-buffer [item]
    (sparse-proto/->nio-index-buffer index-buf))

  sparse-proto/PSparseBuffer
  (set-values! [item new-idx-buf new-data-buf]
    (sparse-proto/set-index-values! index-buf data-buf new-idx-buf
                                    new-data-buf zero-val))

  (set-sequential-values! [item data-buffer]
    (sparse-proto/set-values! item
                              (fu-dtype/make-fastutil-list
                               :int32 (range (dtype/ecount data-buffer)))
                              data-buffer))

  (->sparse-buffer-backing-store [item] item)
  (data-buffer [item]
    (let [start-idx (long (sparse-proto/find-index index-buf 0))
          end-idx (long (sparse-proto/find-index index-buf (dtype/ecount index-buf)))]
      (compute-drv/sub-buffer data-buf start-idx (- end-idx start-idx))))
  (->nio-data-buffer [item]
    (when (= 1 (int (sparse-proto/stride index-buf)))
      (let [start-idx (int (sparse-proto/find-index index-buf 0))
            end-idx (int (sparse-proto/find-index index-buf (dtype/ecount index-buf)))]
        (-> (compute-drv/sub-buffer data-buf
                                    start-idx
                                    (- end-idx start-idx))
            (primitive/->buffer-backing-store)))))
  (index-buffer [item] index-buf)
  (zero-value [item] zero-val)

  compute-drv/PBuffer
  (sub-buffer [buffer offset length]
    (->SparseBuffer data-buf
                    (compute-drv/sub-buffer index-buf offset length)
                    zero-val))
  (alias? [lhs-dev-buffer rhs-dev-buffer]
    (and (instance? SparseBuffer rhs-dev-buffer)
         (compute-drv/alias? (:data-buf lhs-dev-buffer)
                             (:data-buf rhs-dev-buffer))
         (compute-drv/alias? (:index-buf lhs-dev-buffer)
                             (:index-buf rhs-dev-buffer))
         (= (:zero-val lhs-dev-buffer)
            (:zero-val rhs-dev-buffer))))
  (partially-alias? [lhs-dev-buffer rhs-dev-buffer]
    (and (instance? SparseBuffer rhs-dev-buffer)
         (compute-drv/partially-alias? (:data-buf lhs-dev-buffer)
                                       (:data-buf rhs-dev-buffer))
         (compute-drv/partially-alias? (:index-buf lhs-dev-buffer)
                                       (:index-buf rhs-dev-buffer))))

  mp/PElementCount
  (element-count [item] (mp/element-count index-buf))

  primitive/PToArray
  (primitive/->array [item] nil)
  (primitive/->array-copy [item]
    (let [n-elems (mp/element-count item)
          retval (primitive/make-array-of-type (dtype-base/get-datatype item)
                                               n-elems)]
      (dtype/copy! item 0 retval 0 n-elems {:unchecked? true})
      retval))

  dtype-base/PPersistentVector
  (->vector [item]
    (->> (primitive/->array-copy item)
         vec)))


(defn make-sparse-buffer
  ([datatype elem-count-or-seq options]
   (cond
     (number? elem-count-or-seq)
     (->SparseBuffer (fu-dtype/make-fastutil-list datatype 0)
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
       (->SparseBuffer (fu-dtype/make-fastutil-list datatype (map second nonzero-pairs))
                       (sparse-index/make-index-buffer buf-size
                                                       (map first nonzero-pairs))
                       zero-val))))
  ([datatype elem-count-or-seq]
   (make-sparse-buffer datatype elem-count-or-seq {})))


(defn copy-sparse->dense-unchecked!
  "Call at your own risk.  The types have to match, more or less."
  [src src-offset dest dest-offset n-elems options]
  (let [src (-> (sparse-proto/->sparse-buffer-backing-store src)
                (compute-drv/sub-buffer src-offset n-elems))
        dest (compute-drv/sub-buffer dest dest-offset n-elems)]
    ;;Start with the obvious
    (dtype/set-constant! dest 0 (sparse-proto/zero-value src) n-elems)
    (utils/typed-sparse-copy! (sparse-proto/data-buffer src)
                              (primitive/->buffer-backing-store dest)
                              (sparse-proto/index-seq src))
    dest))


(defn copy-dense->sparse-unchecked!
  [src src-offset dest dest-offset n-elems options]
  (let [src (compute-drv/sub-buffer src src-offset n-elems)
        dest (-> (sparse-proto/->sparse-buffer-backing-store dest)
                 (compute-drv/sub-buffer dest-offset n-elems))]
    ;;Not the most efficient way but it will work.
    (dtype/set-constant! dest 0 (sparse-proto/zero-value dest) n-elems)
    (sparse-proto/set-sequential-values! dest src)
    dest))


(defn- generic-sparse->sparse!
  [src dest unchecked?]
  (let [^IntArrayList target-indexes (fu-dtype/make-fastutil-list :int32 0)
        dest-dtype (dtype/get-datatype dest)
        target-data (fu-dtype/make-fastutil-list dest-dtype 0)
        src-data (:data-buf src)]
    ;;Can't think of a faster way at this moment.
    (doseq [[data-idx target-idx] (sparse-proto/index-seq src)]
      (.add target-indexes (int target-idx))
      (fu-dtype/append! target-data (if unchecked?
                                      (-> (dtype/get-value src-data data-idx)
                                          (dtype/unchecked-cast dest-dtype))
                                      (-> (dtype/get-value src-data data-idx)
                                          (dtype/cast dest-dtype)))))
    (sparse-proto/set-values! dest target-indexes target-data)))


(defn copy-sparse->sparse!
  [src src-offset dest dest-offset n-elems {:keys [unchecked?] :as options}]
  (let [src (-> (sparse-proto/->sparse-buffer-backing-store src)
                (compute-drv/sub-buffer src-offset n-elems))
        old-dest dest
        dest (-> (sparse-proto/->sparse-buffer-backing-store dest)
                 (compute-drv/sub-buffer dest-offset n-elems))]
    (dtype/set-constant! dest 0 (sparse-proto/zero-value src) n-elems)
    (if-let [src-nio-indexes (sparse-proto/->nio-index-buffer src)]
      (sparse-proto/set-values! dest
                                src-nio-indexes
                                (sparse-proto/->nio-data-buffer src))
      (generic-sparse->sparse! src dest unchecked?))))


(defn- add-direct-copy-block
  ([src-dtype dest-dtype]
   (dtype-base/add-copy-operation :sparse-buffer :typed-buffer src-dtype
                                  dest-dtype true copy-sparse->dense-unchecked!)
   (dtype-base/add-copy-operation :typed-buffer :sparse-buffer src-dtype
                                  dest-dtype true copy-dense->sparse-unchecked!)
   (dtype-base/add-copy-operation :sparse-buffer :sparse-buffer src-dtype
                                  dest-dtype true copy-sparse->sparse!)
   (when (= src-dtype dest-dtype)
     (dtype-base/add-copy-operation :typed-buffer :sparse-buffer src-dtype
                                    dest-dtype false copy-dense->sparse-unchecked!)
     (dtype-base/add-copy-operation :sparse-buffer :typed-buffer src-dtype
                                    dest-dtype false copy-sparse->dense-unchecked!)
     (dtype-base/add-copy-operation :sparse-buffer :sparse-buffer src-dtype
                                    dest-dtype false copy-sparse->sparse!)))
  ([dtype]
   (add-direct-copy-block dtype dtype)))


(doseq [dtype primitive/datatypes]
  (add-direct-copy-block dtype)
  (when-let [u-dtype (get unsigned/direct-signed->unsigned-map dtype)]
    (add-direct-copy-block u-dtype)
    (add-direct-copy-block u-dtype dtype)
    (add-direct-copy-block dtype u-dtype)))
