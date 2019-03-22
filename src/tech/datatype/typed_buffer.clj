(ns tech.datatype.typed-buffer
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting :as casting]
            [tech.datatype.io :as dtype-io]
            [tech.datatype.base :as base]
            [tech.jna :as jna]
            [tech.parallel :as parallel]
            [tech.datatype.reader :as reader]
            [tech.datatype.writer :as writer]
            [tech.datatype.typecast :as typecast]
            [tech.jna :as jna]
            [clojure.core.matrix.macros :refer [c-for]]
            [clojure.core.matrix.protocols :as mp])
  (:import [com.sun.jna Pointer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)



(defrecord TypedBuffer [datatype backing-store]
  dtype-proto/PDatatype
  (get-datatype [item] datatype)

  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (base/raw-dtype-copy! raw-data ary-target target-offset options))

  dtype-proto/PPersistentVector
  (->vector [item]
    (vec (dtype-proto/->array-copy item)))


  dtype-proto/PPrototype
  (from-prototype [item datatype shape]
    (->TypedBuffer datatype
                   (dtype-proto/from-prototype backing-store
                                               (casting/datatype->host-type datatype)
                                               shape)))
  dtype-proto/PToNioBuffer
  (->buffer-backing-store [item]
    (when (satisfies? dtype-proto/PToNioBuffer backing-store)
      (dtype-proto/->buffer-backing-store backing-store)))

  dtype-proto/PToList
  (->list-backing-store [item]
    (when (satisfies? dtype-proto/PToList backing-store)
      (dtype-proto/->list-backing-store backing-store)))


  dtype-proto/PBuffer
  (sub-buffer [buffer offset length]
    (->TypedBuffer datatype (dtype-proto/sub-buffer backing-store offset length)))
  (alias? [lhs-buffer rhs-buffer]
    (when-let [rhs-nio (typecast/as-nio-buffer rhs-buffer)]
      (dtype-proto/alias? backing-store rhs-nio)))
  (partially-alias? [lhs-buffer rhs-buffer]
    (when-let [rhs-nio (typecast/as-nio-buffer rhs-buffer)]
      (dtype-proto/alias? backing-store rhs-nio)))


  dtype-proto/PToArray
  (->sub-array [item]
    (dtype-proto/->sub-array backing-store))
  (->array-copy [item]
    (if (= datatype (dtype-proto/get-datatype backing-store))
      (dtype-proto/->array-copy backing-store)
      (let [data-buf (dtype-proto/make-container
                      :java-array (casting/datatype->safe-host-type datatype)
                      (base/ecount backing-store)
                      {})]
        (base/copy! item 0 data-buf 0 (base/ecount item)))))

  dtype-proto/PToWriter
  ;;No marshalling/casting on the writer side.
  (->writer-of-type [item writer-datatype unchecked?]
    (if (or (= datatype (dtype-proto/get-datatype backing-store))
            (= datatype writer-datatype))
      (dtype-proto/->writer-of-type backing-store writer-datatype unchecked?)
      ;;We will always check it as it goes into our buffer.
      (-> (dtype-proto/->writer-of-type backing-store datatype unchecked?)
          (dtype-proto/->writer-of-type writer-datatype true))))

  dtype-proto/PToReader
  (->reader-of-type [item reader-datatype unchecked?]
    (if (or (= datatype (dtype-proto/get-datatype backing-store))
            (= datatype reader-datatype))
      (dtype-proto/->reader-of-type backing-store reader-datatype unchecked?)
      ;;We trust that we stored the data correctly.
      (-> (dtype-proto/->reader-of-type backing-store datatype true)
          (dtype-proto/->reader-of-type reader-datatype unchecked?))))


  dtype-proto/PToMutable
  (->mutable-of-type [item mutable-datatype unchecked?]
    (if (or (= datatype (dtype-proto/get-datatype backing-store))
            (= datatype mutable-datatype))
      (dtype-proto/->mutable-of-type backing-store mutable-datatype unchecked?)
      ;;We trust that we stored the data correctly.
      (-> (dtype-proto/->mutable-of-type backing-store datatype true)
          (dtype-proto/->mutable-of-type mutable-datatype unchecked?))))

  jna/PToPtr
  (->ptr-backing-store [item]
    (when (satisfies? jna/PToPtr backing-store)
      (jna/->ptr-backing-store backing-store)))

  mp/PElementCount
  (element-count [item] (mp/element-count backing-store)))


(defn typed-buffer?
  [item]
  (every? #(satisfies? % item)
          [dtype-proto/PDatatype
           dtype-proto/PCopyRawData
           dtype-proto/PPersistentVector dtype-proto/PPrototype
           dtype-proto/PBuffer dtype-proto/PToArray
           dtype-proto/PToWriter dtype-proto/PToReader]))


(defn convertible-to-typed-buffer?
  [item]
  (or (instance? TypedBuffer item)
      (or
       (satisfies? dtype-proto/PToNioBuffer item)
       (satisfies? dtype-proto/PToList))))


(defn convert-to-typed-buffer
  [item]
  (cond
    (instance? TypedBuffer item)
    item
    (satisfies? dtype-proto/PToNioBuffer item)
    (->TypedBuffer (dtype-proto/get-datatype item) (dtype-proto/->buffer-backing-store item))
    (satisfies? dtype-proto/PToList item)
    (->TypedBuffer (dtype-proto/get-datatype item) (dtype-proto/->list-backing-store item))
    :else
    (throw (ex-info "Item is not convertible to typed buffer"
                    {:item-type (type item)}))))


(defn ->typed-buffer
  [item]
  (cond
    (typed-buffer? item)
    item
    :else
    (convert-to-typed-buffer item)))


(defn make-typed-buffer
  ([datatype elem-count-or-seq options]
   (let [host-dtype (casting/datatype->host-datatype datatype)
         backing-store
         (if (or (:unchecked? options)
                 (= host-dtype datatype))
           (dtype-proto/make-container
            :java-array host-dtype elem-count-or-seq options)
           (let [n-elems (if (number? elem-count-or-seq)
                           elem-count-or-seq
                           (base/ecount elem-count-or-seq))
                 container (dtype-proto/make-container :java-array host-dtype n-elems {})]
             (dtype-proto/copy-raw->item! elem-count-or-seq container 0 options)
             container))]
     (->TypedBuffer datatype backing-store)))
  ([datatype elem-count-or-seq]
   (make-typed-buffer datatype elem-count-or-seq {})))


(defn set-datatype
  "Use this one with care."
  [item dtype]
  (assoc (convert-to-typed-buffer item)
         :datatype dtype))


(defmethod dtype-proto/make-container :typed-buffer
  [container-type datatype elem-count-or-seq options]
  (make-typed-buffer datatype elem-count-or-seq options))


(extend-type Object
  dtype-proto/PToTypedBuffer
  (->typed-buffer [item dtype]
    (if (= dtype (dtype-proto/get-datatype item))
      item
      (assoc
       (->typed-buffer item)
       :datatype dtype))))
