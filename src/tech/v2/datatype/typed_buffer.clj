(ns tech.v2.datatype.typed-buffer
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.base :as base]
            [tech.jna :as jna]
            [tech.v2.datatype.reader :as reader]
            [tech.v2.datatype.writer :as writer]
            [tech.v2.datatype.mutable :as mutable]
            [tech.v2.datatype.pprint :as dtype-pprint])
  (:import [com.sun.jna Pointer]
           [java.io Writer]
           [tech.v2.datatype.protocols PDatatype]
           [tech.v2.datatype ObjectReader ObjectWriter]
           [clojure.lang Counted Indexed]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)



(deftype TypedBuffer [datatype backing-store]
  dtype-proto/PDatatype
  (get-datatype [item] datatype)

  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (base/raw-dtype-copy! raw-data ary-target target-offset options))


  dtype-proto/PPrototype
  (from-prototype [item datatype shape]
    (TypedBuffer. datatype
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
    (TypedBuffer. datatype (dtype-proto/sub-buffer backing-store offset length)))


  dtype-proto/PToArray
  (->sub-array [item]
    (when (= datatype (dtype-proto/get-datatype backing-store))
      (dtype-proto/->sub-array backing-store)))
  (->array-copy [item]
    (if (= datatype (dtype-proto/get-datatype backing-store))
      (dtype-proto/->array-copy backing-store)
      (let [data-buf (dtype-proto/make-container
                      :java-array (casting/datatype->safe-host-type datatype)
                      (base/ecount backing-store)
                      {})]
        (base/copy! item 0 data-buf 0 (base/ecount item)))))

  dtype-proto/PToWriter
  (convertible-to-writer? [item] (dtype-proto/convertible-to-writer? backing-store))
  ;;No marshalling/casting on the writer side.
  (->writer [item options]
    (let [{writer-datatype :datatype
           unchecked? :unchecked?} options
          writer-datatype (or writer-datatype datatype)
          writer-matches? (= writer-datatype datatype)
          src-writer-unchecked? (if writer-matches?
                                  unchecked?
                                  false)
          direct-writer (cond
                          (dtype-proto/as-nio-buffer backing-store)
                          (writer/make-buffer-writer item
                                                     (casting/safe-flatten datatype)
                                                     datatype
                                                     src-writer-unchecked?)
                          (dtype-proto/as-list backing-store)
                          (writer/make-list-writer item
                                                   (casting/safe-flatten datatype)
                                                   datatype
                                                   src-writer-unchecked?)
                          :else
                          (dtype-proto/->writer backing-store {:datatype datatype}))]
      (cond-> direct-writer
        (not writer-matches?)
        (dtype-proto/->writer {:datatype writer-datatype :unchecked? unchecked?}))))


  dtype-proto/PToReader
  (convertible-to-reader? [item] (dtype-proto/convertible-to-reader? backing-store))
  (->reader [item options]
    (let [{reader-datatype :datatype
           unchecked? :unchecked?} options
          reader-datatype (or reader-datatype datatype)
          src-unchecked? true
          ;;There is an unchecked fastpath that does not attempt to do elementwise
          ;;conversions of the data in the buffer.
          [intermediate-datatype src-datatype]
          (if (and unchecked?
                   (= reader-datatype (base/get-datatype backing-store)))
            [reader-datatype reader-datatype]
            [datatype (casting/safe-flatten datatype)])
          direct-reader (cond
                          (dtype-proto/as-nio-buffer backing-store)
                          (reader/make-buffer-reader item
                                                     src-datatype
                                                     intermediate-datatype
                                                     src-unchecked?)
                          (dtype-proto/as-list backing-store)
                          (reader/make-list-reader item
                                                   src-datatype
                                                   intermediate-datatype
                                                   src-unchecked?)
                          :else
                          (dtype-proto/->reader backing-store
                                                {:datatype datatype
                                                 :unchecked? unchecked?}))]
      (cond-> direct-reader
        (not= reader-datatype datatype)
        (dtype-proto/->reader {:datatype reader-datatype
                               :unchecked? unchecked?}))))


  Counted
  (count [item] (base/ecount item))


  Indexed
  (nth [item idx]
    (base/get-value item idx))

  (nth [item idx def-val]
    (if (< idx (base/ecount item))
      (nth item idx)
      def-val))


  dtype-proto/PToIterable
  (convertible-to-iterable? [item] true)
  (->iterable [item options] (dtype-proto/->reader item options))


  dtype-proto/PToMutable
  (convertible-to-mutable? [item]
    (dtype-proto/convertible-to-mutable? backing-store))
  (->mutable [item options]
    (let [{mutable-datatype :datatype
           unchecked? :unchecked?} options
          mutable-datatype (or mutable-datatype datatype)
          src-unchecked? (if (= mutable-datatype datatype)
                           unchecked?
                           false)
          direct-mutable (cond
                           (dtype-proto/convertible-to-fastutil-list? backing-store)
                           (mutable/make-list-mutable item
                                                      (casting/safe-flatten datatype)
                                                      datatype
                                                      src-unchecked?)
                           :else
                           (dtype-proto/->mutable backing-store
                                                  {:datatype datatype
                                                   :unchecked? src-unchecked?}))]
      (cond-> direct-mutable
        (not= mutable-datatype datatype)
        (dtype-proto/->mutable {:datatype mutable-datatype
                                :unchecked? unchecked?}))))


  dtype-proto/PRemoveRange
  (remove-range! [item idx count]
    (dtype-proto/remove-range! backing-store idx count))


  dtype-proto/PInsertBlock
  (insert-block! [item idx values options]
    (dtype-proto/insert-block! backing-store
                               idx
                               (if (:unchecked? options)
                                 values
                                 (dtype-proto/->reader values {:datatype datatype}))
                               options))

  dtype-proto/PToJNAPointer
  (convertible-to-data-ptr? [item]
    (dtype-proto/convertible-to-data-ptr? backing-store))
  (->jna-ptr [item] (dtype-proto/->jna-ptr backing-store))


  dtype-proto/PToBufferDesc
  (convertible-to-buffer-desc? [item]
    (when (and (casting/numeric-type? datatype)
               (= (casting/numeric-byte-width datatype)
                  (casting/numeric-byte-width (dtype-proto/get-datatype
                                               backing-store))))
      (dtype-proto/convertible-to-buffer-desc? backing-store)))
  (->buffer-descriptor [item]
    (when (and (casting/numeric-type? datatype)
               (= (casting/numeric-byte-width datatype)
                  (casting/numeric-byte-width (dtype-proto/get-datatype
                                               backing-store))))
      (-> (dtype-proto/->buffer-descriptor backing-store)
          (assoc :datatype datatype))))

  dtype-proto/PCountable
  (ecount [item] (dtype-proto/ecount backing-store))

  Object
  (toString [this]
    (let [n-items (base/ecount this)
          format-str (if (> n-items 20)
                       "#tech.v2.datatype.typed-buffer<%s,%s>%s\n[%s...]"
                       "#tech.v2.datatype.typed-buffer<%s,%s>%s\n[%s]"
                       )]
      (format format-str
              (.getName ^Class (type backing-store))
              (name datatype)
              [n-items]
              (-> (dtype-proto/sub-buffer this 0 (min 20 (base/ecount this)))
                  (dtype-pprint/print-reader-data)))))
  (hashCode [this]
    (.hashCode {:datatype datatype
                :backing-store backing-store}))
  (equals [this other]
    (.equals other {:datatype datatype
                    :backing-store backing-store})))


(defmethod print-method TypedBuffer
  [buf w]
  (.write ^Writer w (.toString ^Object buf)))


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
       (dtype-proto/base-type-convertible? item))))


(defn convert-to-typed-buffer
  [item]
  (cond
    (instance? TypedBuffer item)
    item
    (dtype-proto/base-type-convertible? item)
    (TypedBuffer. (dtype-proto/get-datatype item)
                   (or (dtype-proto/as-nio-buffer item)
                       (dtype-proto/as-list item)))
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
   (let [host-dtype (casting/datatype->host-datatype datatype)]
     (if (or (:unchecked? options)
             (= host-dtype datatype))
       (TypedBuffer. datatype
                      (dtype-proto/make-container
                       :java-array host-dtype elem-count-or-seq options))
       (let [n-elems (if (number? elem-count-or-seq)
                           elem-count-or-seq
                           (base/ecount elem-count-or-seq))
             container (dtype-proto/make-container :java-array host-dtype
                                                   n-elems {})
             typed-buf (TypedBuffer. datatype container)]
         (when-not (number? elem-count-or-seq)
           (dtype-proto/copy-raw->item! elem-count-or-seq
                                        typed-buf 0 options))


         typed-buf))))
  ([datatype elem-count-or-seq]
   (make-typed-buffer datatype elem-count-or-seq {})))


(defn set-datatype
  "Use this one with care."
  [item dtype]
  (if (= dtype (dtype-proto/get-datatype item))
    item
    (let [^TypedBuffer item (convert-to-typed-buffer item)]
      (TypedBuffer. dtype (.backing-store item)))))


(defmethod dtype-proto/make-container :typed-buffer
  [_container-type datatype elem-count-or-seq options]
  (make-typed-buffer datatype elem-count-or-seq options))
