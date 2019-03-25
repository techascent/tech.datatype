(ns tech.datatype.writer
  (:require [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.nio-access
             :refer [buf-put buf-get
                     datatype->pos-fn
                     datatype->read-fn
                     datatype->write-fn
                     unchecked-full-cast
                     checked-full-read-cast
                     checked-full-write-cast
                     nio-type? list-type?
                     cls-type->read-fn
                     cls-type->write-fn
                     cls-type->pos-fn]]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.parallel :as parallel]
            [tech.jna :as jna]
            [clojure.core.matrix :as m]
            [clojure.core.matrix.protocols :as mp]
            [tech.datatype.typecast :refer :all]
            [tech.datatype.fast-copy :as fast-copy]
            [tech.datatype.typecast :as typecast])
  (:import [tech.datatype ObjectWriter ByteWriter
            ShortWriter IntWriter LongWriter
            FloatWriter DoubleWriter BooleanWriter]
           [java.nio Buffer ByteBuffer ShortBuffer
            IntBuffer LongBuffer FloatBuffer DoubleBuffer]
           [it.unimi.dsi.fastutil.bytes ByteList ByteArrayList]
           [it.unimi.dsi.fastutil.shorts ShortList ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntList IntArrayList]
           [it.unimi.dsi.fastutil.longs LongList LongArrayList]
           [it.unimi.dsi.fastutil.floats FloatList FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleList DoubleArrayList]
           [it.unimi.dsi.fastutil.booleans BooleanList BooleanArrayList]
           [it.unimi.dsi.fastutil.objects ObjectList ObjectArrayList]
           [com.sun.jna Pointer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro make-buffer-writer
  "Make a writer from a nio buffer or a fastutil list backing store.  "
  [writer-type buffer-type buffer buffer-pos
   writer-datatype
   intermediate-datatype
   buffer-datatype
   unchecked?]
  `(if ~unchecked?
     (reify
       ~writer-type
       (getDatatype [writer#] ~intermediate-datatype)
       (size [writer#] (int (mp/element-count ~buffer)))
       (write [writer# idx# value#]
         (cls-type->write-fn ~buffer-type ~buffer idx# ~buffer-pos
          (unchecked-full-cast value# ~writer-datatype
                               ~intermediate-datatype
                               ~buffer-datatype)))
       (invoke [item# idx# value#] (.write item# (int idx#)
                                           (casting/datatype->unchecked-cast-fn
                                            :unknown
                                            ~intermediate-datatype
                                            value#)))
       dtype-proto/PToNioBuffer
       (->buffer-backing-store [reader#]
         (dtype-proto/->buffer-backing-store ~buffer))
       dtype-proto/PToList
       (->list-backing-store [reader#]
         (dtype-proto/->list-backing-store ~buffer))
       dtype-proto/PBuffer
       (sub-buffer [buffer# offset# length#]
         (-> (dtype-proto/sub-buffer ~buffer offset# length#)
             (dtype-proto/->reader-of-type ~intermediate-datatype ~unchecked?)))
       (alias? [buffer# rhs#]
         (dtype-proto/alias? ~buffer rhs#))
       (partially-alias? [lhs# rhs#]
         (dtype-proto/partially-alias? ~buffer rhs#))
       dtype-proto/PSetConstant
       (set-constant! [item# offset# value# elem-count#]
         (dtype-proto/set-constant! ~buffer offset#
                                    (casting/cast value# ~intermediate-datatype)
                                    elem-count#)))
     (reify ~writer-type
       (getDatatype [writer#] ~intermediate-datatype)
       (size [writer#] (int (mp/element-count ~buffer)))
       (write [writer# idx# value#]
         (cls-type->write-fn ~buffer-type ~buffer idx# ~buffer-pos
                             (checked-full-write-cast value# ~writer-datatype
                                                      ~intermediate-datatype
                                                      ~buffer-datatype)))
       (invoke [item# idx# value#] (.write item# (int idx#)
                                           (casting/datatype->cast-fn
                                            :unknown
                                            ~intermediate-datatype
                                            value#)))
       dtype-proto/PToNioBuffer
       (->buffer-backing-store [reader#]
         (dtype-proto/->buffer-backing-store ~buffer))
       dtype-proto/PToList
       (->list-backing-store [reader#]
         (dtype-proto/->list-backing-store ~buffer))
       dtype-proto/PBuffer
       (sub-buffer [buffer# offset# length#]
         (-> (dtype-proto/sub-buffer ~buffer offset# length#)
             (dtype-proto/->reader-of-type ~intermediate-datatype ~unchecked?)))
       (alias? [buffer# rhs#]
         (dtype-proto/alias? ~buffer rhs#))
       (partially-alias? [lhs# rhs#]
         (dtype-proto/partially-alias? ~buffer rhs#))
       dtype-proto/PSetConstant
       (set-constant! [item# offset# value# elem-count#]
         (dtype-proto/set-constant! ~buffer offset#
                                    (casting/cast value# ~intermediate-datatype)
                                    elem-count#)))))


(defmacro make-buffer-writer-table
  []
  `(->> [~@(for [dtype casting/numeric-types]
             (let [buffer-datatype (casting/datatype->host-datatype dtype)]
               [[buffer-datatype dtype]
                `(fn [buffer# unchecked?#]
                   (let [buffer# (typecast/datatype->buffer-cast-fn ~buffer-datatype buffer#)
                         buffer-pos# (datatype->pos-fn ~buffer-datatype buffer#)]
                     (make-buffer-writer
                      ~(typecast/datatype->writer-type dtype)
                      ~(typecast/datatype->buffer-type buffer-datatype)
                      buffer# buffer-pos#
                      ~(casting/datatype->safe-host-type dtype) ~dtype
                      ~buffer-datatype
                      unchecked?#)))]))]
        (into {})))



(def buffer-writer-table (make-buffer-writer-table))


(defn make-buffer-writer
  [item datatype unchecked?]
  (let [nio-buffer (dtype-proto/->buffer-backing-store item)
        item-dtype (dtype-proto/get-datatype item)
        buffer-dtype (dtype-proto/get-datatype nio-buffer)
        no-translate-writer (get buffer-writer-table [buffer-dtype item-dtype])
        translate-writer (get buffer-writer-table [buffer-dtype buffer-dtype])]
    (if no-translate-writer
      (no-translate-writer nio-buffer unchecked?)
      (-> (translate-writer nio-buffer true)
          (dtype-proto/->writer-of-type datatype unchecked?)))))


(defmacro make-list-writer-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             (let [buffer-datatype (casting/datatype->host-datatype dtype)]
               [[buffer-datatype dtype]
                `(fn [buffer# unchecked?#]
                   (let [buffer# (typecast/datatype->list-cast-fn ~buffer-datatype buffer#)]
                     (make-buffer-writer
                      ~(typecast/datatype->writer-type dtype)
                      ~(typecast/datatype->list-type buffer-datatype)
                      buffer# 0
                      ~(casting/datatype->safe-host-type dtype) ~dtype
                      ~buffer-datatype
                      unchecked?#)))]))]
        (into {})))


(def list-writer-table (make-list-writer-table))


(defn make-list-writer
  [item datatype unchecked?]
  (let [nio-list (dtype-proto/->list-backing-store item)
        item-dtype (dtype-proto/get-datatype item)
        list-dtype (dtype-proto/get-datatype nio-list)
        no-translate-writer (get list-writer-table [list-dtype item-dtype])
        translate-writer (get list-writer-table [list-dtype list-dtype])]
    (if no-translate-writer
      (no-translate-writer nio-list unchecked?)
      (-> (translate-writer nio-list true)
          (dtype-proto/->writer-of-type datatype unchecked?)))))


(defn- make-object-wrapper
  [writer datatype]
  (let [item-dtype (dtype-proto/get-datatype writer)]
    (when-not (and (= :object (casting/flatten-datatype item-dtype))
                   (= :object (casting/flatten-datatype datatype)))
      (throw (ex-info "Incorrect use of object wrapper" {}))))
  (if (= datatype (dtype-proto/get-datatype writer))
    writer
    (let [obj-writer (typecast/datatype->writer :object writer)]
      (reify
        ObjectWriter
        (getDatatype [_] datatype)
        (size [_] (.size obj-writer))
        (write [_ idx value] (.write obj-writer idx value))
        (invoke [_ idx value] (.write obj-writer idx value))
        dtype-proto/PToNioBuffer
        (->buffer-backing-store [writer]
          (dtype-proto/->buffer-backing-store obj-writer))
       dtype-proto/PToList
       (->list-backing-store [writer]
         (dtype-proto/->list-backing-store obj-writer))
       dtype-proto/PBuffer
       (sub-buffer [writer offset length]
         (-> (dtype-proto/sub-buffer obj-writer offset length)
             (dtype-proto/->writer-of-type datatype true)))
       (alias? [writer rhs]
         (dtype-proto/alias? obj-writer rhs))
       (partially-alias? [writer rhs]
         (dtype-proto/partially-alias? obj-writer rhs))))))


(defmacro make-marshalling-writer
  [dst-writer result-dtype intermediate-dtype src-dtype src-writer-type
   unchecked?]
  `(if ~unchecked?
     (reify ~src-writer-type
       (getDatatype [item#] ~intermediate-dtype)
       (size [item#] (.size ~dst-writer))
       (write[item# idx# value#]
         (.write ~dst-writer idx#
                 (unchecked-full-cast value# ~src-dtype ~intermediate-dtype ~result-dtype)))
       (invoke [_ idx# value#] (.write ~dst-writer idx#
                                       (unchecked-full-cast value# :unknown ~intermediate-dtype ~result-dtype)))
       dtype-proto/PToNioBuffer
       (->buffer-backing-store [writer#]
         (dtype-proto/->buffer-backing-store ~dst-writer))
       dtype-proto/PToList
       (->list-backing-store [writer#]
         (dtype-proto/->list-backing-store ~dst-writer))
       dtype-proto/PBuffer
       (sub-buffer [writer# offset# length#]
         (-> (dtype-proto/sub-buffer ~dst-writer offset# length#)
             (dtype-proto/->writer-of-type ~intermediate-dtype ~unchecked?)))
       (alias? [writer# rhs#]
         (dtype-proto/alias? ~dst-writer rhs#))
       (partially-alias? [writer# rhs#]
         (dtype-proto/partially-alias? ~dst-writer rhs#)))
     (reify ~src-writer-type
       (getDatatype [item#] ~intermediate-dtype)
       (size [item#] (.size ~dst-writer))
       (write[item# idx# value#]
         (.write ~dst-writer idx#
                 (checked-full-write-cast value# ~src-dtype ~intermediate-dtype ~result-dtype)))
       (invoke [_ idx# value#] (.write ~dst-writer idx#
                                       (checked-full-write-cast value# :unknown
                                                                ~intermediate-dtype ~result-dtype)))
       dtype-proto/PToNioBuffer
       (->buffer-backing-store [writer#]
         (dtype-proto/->buffer-backing-store ~dst-writer))
       dtype-proto/PToList
       (->list-backing-store [writer#]
         (dtype-proto/->list-backing-store ~dst-writer))
       dtype-proto/PBuffer
       (sub-buffer [writer# offset# length#]
         (-> (dtype-proto/sub-buffer ~dst-writer offset# length#)
             (dtype-proto/->writer-of-type ~intermediate-dtype ~unchecked?)))
       (alias? [writer# rhs#]
         (dtype-proto/alias? ~dst-writer rhs#))
       (partially-alias? [writer# rhs#]
         (dtype-proto/partially-alias? ~dst-writer rhs#)))))


(defmacro make-marshalling-writer-table
  []
  `(->> [~@(for [dtype (casting/all-datatypes)
                 dst-writer-datatype casting/all-host-datatypes]
            [[dst-writer-datatype dtype]
             `(fn [dst-writer# unchecked?#]
                (let [dst-writer# (typecast/datatype->writer ~dst-writer-datatype dst-writer# true)]
                  (make-marshalling-writer
                   dst-writer#
                   ~dst-writer-datatype
                   ~dtype
                   ~(casting/datatype->safe-host-type dtype)
                   ~(typecast/datatype->writer-type (casting/datatype->safe-host-type dtype))
                   unchecked?#)))])]
        (into {})))


(def marshalling-writer-table (make-marshalling-writer-table))


(defn make-marshalling-writer
  [dst-writer src-dtype unchecked?]
  (let [dst-dtype (dtype-proto/get-datatype dst-writer)]
    (if (= dst-dtype src-dtype)
      dst-writer
      (let [dst-writer (if (= (casting/flatten-datatype dst-dtype)
                              (casting/flatten-datatype src-dtype))
                         dst-writer
                         (let [writer-fn (get marshalling-writer-table
                                              [(casting/flatten-datatype dst-dtype)
                                               (casting/flatten-datatype src-dtype)])]
                           (writer-fn dst-writer (casting/flatten-datatype src-dtype))))
            dst-dtype (dtype-proto/get-datatype dst-writer)]
        (if (not= dst-dtype src-dtype)
          (make-object-wrapper dst-writer src-dtype)
          dst-writer)))))



(defmacro extend-writer-type
  [writer-type datatype]
  `(clojure.core/extend
       ~writer-type
     dtype-proto/PToWriter
     {:->writer-of-type
      (fn [item# dtype# unchecked?#]
        (make-marshalling-writer item# dtype# unchecked?#))}))


(extend-writer-type ByteWriter :int8)
(extend-writer-type ShortWriter :int16)
(extend-writer-type IntWriter :int32)
(extend-writer-type LongWriter :int64)
(extend-writer-type FloatWriter :float32)
(extend-writer-type DoubleWriter :float64)
(extend-writer-type BooleanWriter :boolean)
(extend-writer-type ObjectWriter :object)


(defmacro make-indexed-writer-impl
  [datatype writer-type indexes values unchecked?]
  `(let [idx-reader# (datatype->reader :int32 ~indexes true)
         values# (datatype->writer ~datatype ~values ~unchecked?)]
     (reify ~writer-type
       (getDatatype [item#] ~datatype)
       (size [item#] (int (mp/element-count ~indexes)))
       (write [item# idx# value#]
         (.write values# (.read idx-reader# idx#) value#))
       (invoke [item# idx# value#]
         (.write item# (int idx#)
                 (casting/datatype->unchecked-cast-fn
                  :unknown ~datatype value#))))))


(defmacro make-indexed-writer-creators
  []
  `(->> [~@(for [dtype (casting/all-datatypes)]
             [dtype `(fn [indexes# values# unchecked?#]
                       (make-indexed-writer
                        ~dtype ~(typecast/datatype->writer-type dtype)
                        indexes# values# unchecked?#))])]
        (into {})))

(def indexed-writer-creators (make-indexed-writer-creators))


(defn make-indexed-writer
  [indexes values & {:keys [datatype unchecked?]}]
  (let [datatype (or datatype (dtype-proto/get-datatype values))
        writer-fn (get indexed-writer-creators (casting/flatten-datatype datatype))]
    (writer-fn indexes values unchecked?)))


(defmacro make-iterable-write-fn
  [datatype]
  `(fn [dst# src# unchecked?#]
     (let [dst-writer# (typecast/datatype->writer ~datatype dst# true)
           src-iter# (typecast/datatype->iter ~datatype src# unchecked?#)]
       (loop [idx# (int 0)]
         (if (.hasNext src-iter#)
           (do
             (.write dst-writer# idx# (typecast/datatype->iter-next-fn ~datatype src-iter#))
             (recur (unchecked-inc idx#)))
           dst#)))))


(defmacro make-iterable-writer-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             [dtype `(make-iterable-write-fn ~dtype)])]
        (into {})))


(def iterable-writer-table (make-iterable-writer-table))


(defn iterable->writer
  [dst-writer src-iterable & {:keys [datatype unchecked?]}]
  (let [datatype (or datatype (dtype-proto/get-datatype src-iterable))
        writer-fn (get iterable-writer-table (casting/flatten-datatype datatype))]
    (writer-fn dst-writer src-iterable unchecked?)))


(defmacro typed-write
  [datatype item idx value]
  `(.write (typecast/datatype->writer ~datatype ~item)
           ~idx ~value))
