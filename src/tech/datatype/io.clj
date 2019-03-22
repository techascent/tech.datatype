(ns tech.datatype.io
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting
             :refer [numeric-type? integer-type? numeric-byte-width
                     datatype->host-type]
             :as casting]
            [tech.jna :as jna]
            [tech.parallel :as parallel]
            [clojure.set :as c-set]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.datatype.fast-copy :as fast-copy]
            [tech.datatype.typecast
             :refer [datatype->reader
                     datatype->writer]
             :as typecast]
            [clojure.core.matrix.protocols :as mp])

  (:import [tech.datatype
            ObjectReader ObjectWriter ObjectMutable
            ByteReader ByteWriter ByteMutable
            ShortReader ShortWriter ShortMutable
            IntReader IntWriter IntMutable
            LongReader LongWriter LongMutable
            FloatReader FloatWriter FloatMutable
            DoubleReader DoubleWriter DoubleMutable
            BooleanReader BooleanWriter BooleanMutable]
           [com.sun.jna Pointer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn dense-copy!
  [dst src unchecked?]
  (let [dst-dtype (dtype-proto/get-datatype dst)
        src-dtype (dtype-proto/get-datatype src)
        src-buf (or (typecast/as-nio-buffer src)
                    (typecast/as-list src))
        dst-nio (typecast/as-nio-buffer dst)
        dst-list (typecast/as-list dst)
        dst-buf (or dst-nio dst-list)
        src-buf-dtype (when src-buf (dtype-proto/get-datatype src-buf))
        dst-buf-dtype (when dst-buf (dtype-proto/get-datatype dst-buf))
        fast-path? (and src-buf
                        dst-buf
                        (or (and (numeric-type? dst-dtype)
                                 (= dst-dtype src-dtype)
                                 (= dst-buf-dtype dst-dtype)
                                 (= src-buf-dtype src-dtype))
                            (and (integer-type? dst-dtype)
                                 (integer-type? src-dtype)
                                 (= (casting/datatype->host-datatype dst-dtype) dst-buf-dtype)
                                 (= (casting/datatype->host-datatype src-dtype) src-buf-dtype)
                                 unchecked?
                                 (= (numeric-byte-width dst-dtype)
                                    (numeric-byte-width src-dtype)))))
        dst-type-matches? (or (= dst-buf-dtype dst-dtype)
                              (and unchecked?
                                   (= (casting/datatype->host-datatype dst-dtype)
                                      dst-buf-dtype)))]
    ;;Fast path means no conversion is necessary and we can hit optimized
    ;;bulk pathways
    (if fast-path?
      ;;The only real special case is if one side is a ptr
      ;;and the other is an array
      (fast-copy/copy! dst src)
      (cond
        (and dst-nio dst-type-matches?)
        (fast-copy/parallel-nio-write! dst-nio src unchecked?)
        (and dst-list dst-type-matches?)
        (fast-copy/parallel-list-write! dst-list src unchecked?)
        :else
        (fast-copy/parallel-slow-copy! dst src unchecked?))))
  dst)

(defmacro make-indexed-reader
  [datatype reader-type indexes values unchecked?]
  `(let [idx-reader# (datatype->reader :int32 ~indexes true)
         values# (datatype->reader ~datatype ~values ~unchecked?)]
     (reify ~reader-type
       (getDatatype [item#] ~datatype)
       (size [item#] (int (mp/element-count ~indexes)))
       (read [item# idx#]
         (.read values# (.read idx-reader# idx#))))))

(defmacro make-indexed-reader-creators
  []
  `(->> [~@(for [dtype (casting/all-datatypes)]
             [dtype `(fn [indexes# values# unchecked?#]
                       (make-indexed-reader
                        ~dtype ~(typecast/datatype->reader-type dtype)
                        indexes# values# unchecked?#))])]
        (into {})))

(def indexed-reader-creators (make-indexed-reader-creators))

(defmacro make-indexed-writer
  [datatype writer-type indexes values unchecked?]
  `(let [idx-reader# (datatype->reader :int32 ~indexes true)
         values# (datatype->writer ~datatype ~values ~unchecked?)]
     (reify ~writer-type
       (getDatatype [item#] ~datatype)
       (size [item#] (int (mp/element-count ~indexes)))
       (write [item# idx# value#]
         (.write values# (.read idx-reader# idx#) value#)))))

(defmacro make-indexed-writer-creators
  []
  `(->> [~@(for [dtype (casting/all-datatypes)]
             [dtype `(fn [indexes# values# unchecked?#]
                       (make-indexed-writer
                        ~dtype ~(typecast/datatype->writer-type dtype)
                        indexes# values# unchecked?#))])]
        (into {})))

(def indexed-writer-creators (make-indexed-writer-creators))

(defn write-indexes!
  [datatype item indexes values options]
  (let [unchecked? (:unchecked? options)
        typed-write-fn (get indexed-writer-creators datatype)]
    (when-not typed-write-fn
      (throw (ex-info "Failed to create indexed reader" {:datatype datatype})))
    (fast-copy/parallel-read! (typed-write-fn indexes item unchecked?) values true)
    item))

(defn read-indexes!
  [datatype item indexes values options]
  (let [unchecked? (:unchecked? options)
        typed-reader-fn (get indexed-reader-creators datatype)]
    (when-not typed-reader-fn
      (throw (ex-info "Failed to create indexed reader" {:datatype datatype})))
    (fast-copy/parallel-write! values (typed-reader-fn indexes item unchecked?) true)
    values))
