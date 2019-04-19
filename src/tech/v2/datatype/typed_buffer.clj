(ns tech.v2.datatype.typed-buffer
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.io :as dtype-io]
            [tech.v2.datatype.base :as base]
            [tech.jna :as jna]
            [tech.parallel :as parallel]
            [tech.v2.datatype.reader :as reader]
            [tech.v2.datatype.writer :as writer]
            [tech.v2.datatype.typecast :as typecast]
            [tech.jna :as jna]
            [clojure.core.matrix.macros :refer [c-for]]
            [clojure.core.matrix.protocols :as mp])
  (:import [com.sun.jna Pointer]
           [tech.v2.datatype.protocols PDatatype]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)



(defrecord TypedBuffer [datatype backing-store]
  dtype-proto/PDatatype
  (get-datatype [item] datatype)

  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (base/raw-dtype-copy! raw-data ary-target target-offset options))


  dtype-proto/PPrototype
  (from-prototype [item datatype shape]
    (->TypedBuffer datatype
                   (dtype-proto/from-prototype backing-store
                                               (casting/datatype->host-type datatype)
                                               shape)))

  dtype-proto/PToBackingStore
  (->backing-store-seq [item]
    (dtype-proto/->backing-store-seq backing-store))

  dtype-proto/PToNioBuffer
  (convertible-to-nio-buffer? [item]
    (dtype-proto/nio-convertible? backing-store))
  (->buffer-backing-store [item]
    (dtype-proto/as-nio-buffer backing-store))

  dtype-proto/PToList
  (convertible-to-fastutil-list? [item]
    (dtype-proto/list-convertible? backing-store))
  (->list-backing-store [item]
    (when (satisfies? dtype-proto/PToList backing-store)
      (dtype-proto/->list-backing-store backing-store)))


  dtype-proto/PSetConstant
  (set-constant! [item offset value n-elems]
    (let [value (-> value
                    (casting/cast datatype)
                    (casting/unchecked-cast (dtype-proto/get-datatype
                                             backing-store)))]
      (dtype-proto/set-constant! backing-store offset value n-elems)))


  dtype-proto/PBuffer
  (sub-buffer [buffer offset length]
    (->TypedBuffer datatype (dtype-proto/sub-buffer backing-store offset length)))


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
    (let [writer-matches? (= writer-datatype datatype)
          src-writer-unchecked? (if writer-matches?
                                  unchecked?
                                  false)
          direct-writer (cond
                          (dtype-proto/as-nio-buffer backing-store)
                          (writer/make-buffer-writer item src-writer-unchecked?)
                          (dtype-proto/as-list backing-store)
                          (writer/make-list-writer item src-writer-unchecked?)
                          :else
                          (dtype-proto/->writer-of-type backing-store datatype false))]
      (cond-> direct-writer
        (not writer-matches?)
        (dtype-proto/->writer-of-type writer-datatype unchecked?))))

  dtype-proto/PToReader
  (->reader-of-type [item reader-datatype unchecked?]
    (let [direct-reader (cond
                          (dtype-proto/as-nio-buffer backing-store)
                          (reader/make-buffer-reader item)
                          (dtype-proto/as-list backing-store)
                          (reader/make-list-reader item)
                          :else
                          (dtype-proto/->reader-of-type backing-store
                                                        datatype unchecked?))]
      (cond-> direct-reader
        (not= reader-datatype datatype)
        (dtype-proto/->reader-of-type reader-datatype unchecked?))))


  dtype-proto/PToMutable
  (->mutable-of-type [item mutable-datatype unchecked?]
    (-> (dtype-proto/->mutable-of-type
         backing-store datatype (if (= mutable-datatype datatype)
                                  unchecked?
                                  false))
        (dtype-proto/->mutable-of-type mutable-datatype unchecked?)))


  dtype-proto/PRemoveRange
  (remove-range! [item idx count]
    (dtype-proto/remove-range! backing-store idx count))


  dtype-proto/PInsertBlock
  (insert-block! [item idx values options]
    (dtype-proto/insert-block! backing-store
                               idx
                               (if (:unchecked? options)
                                 values
                                 (dtype-proto/->reader-of-type values datatype false))
                               options))

  jna/PToPtr
  (is-jna-ptr-convertible? [item]
    (jna/ptr-convertible? backing-store))
  (->ptr-backing-store [item]
    (jna/as-ptr backing-store))

  mp/PElementCount
  (element-count [item] (mp/element-count backing-store)))


(defn typed-buffer?
  [item]
  (every? #(satisfies? % item)
          [dtype-proto/PDatatype
           dtype-proto/PCopyRawData
           dtype-proto/PPrototype
           dtype-proto/PBuffer
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
    (->TypedBuffer (dtype-proto/get-datatype item) item)
    (satisfies? dtype-proto/PToList item)
    (->TypedBuffer (dtype-proto/get-datatype item) item)
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
                 container (dtype-proto/make-container :java-array host-dtype
                                                       n-elems {})]
             (when-not (number? elem-count-or-seq)
               (dtype-proto/copy-raw->item! elem-count-or-seq container 0 options))
             container))]
     (->TypedBuffer datatype backing-store)))
  ([datatype elem-count-or-seq]
   (make-typed-buffer datatype elem-count-or-seq {})))


(defn set-datatype
  "Use this one with care."
  [item dtype]
  (if (= dtype (dtype-proto/get-datatype item))
    item
    (assoc (convert-to-typed-buffer item)
           :datatype dtype)))


(defmethod dtype-proto/make-container :typed-buffer
  [container-type datatype elem-count-or-seq options]
  (make-typed-buffer datatype elem-count-or-seq options))
