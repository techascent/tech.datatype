(ns tech.v2.datatype.writer
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.nio-access
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
            [tech.v2.datatype.typecast :refer :all]
            [tech.v2.datatype.fast-copy :as fast-copy]
            [tech.v2.datatype.typecast :as typecast])
  (:import [tech.v2.datatype ObjectWriter ByteWriter
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


(defmacro make-buffer-writer-impl
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
       (lsize [writer#] (int (mp/element-count ~buffer)))
       (write [writer# idx# value#]
         (cls-type->write-fn ~buffer-type ~buffer idx# ~buffer-pos
          (unchecked-full-cast value# ~writer-datatype
                               ~intermediate-datatype
                               ~buffer-datatype)))
       dtype-proto/PToNioBuffer
       (convertible-to-nio-buffer? [writer#]
         (dtype-proto/nio-convertible? ~buffer))
       (->buffer-backing-store [writer#]
         (dtype-proto/as-nio-buffer ~buffer))
       dtype-proto/PToList
       (convertible-to-fastutil-list? [writer#]
         (dtype-proto/list-convertible? ~buffer))
       (->list-backing-store [writer#]
         (dtype-proto/as-list ~buffer))
       jna/PToPtr
       (is-jna-ptr-convertible? [writer#]
         (jna/ptr-convertible? ~buffer))
       (->ptr-backing-store [writer#]
         (jna/as-ptr ~buffer))
       dtype-proto/PToArray
       (->sub-array [reader#]
         (dtype-proto/->sub-array ~buffer))
       (->array-copy [reader#]
         (dtype-proto/->array-copy ~buffer))

       dtype-proto/PBuffer
       (sub-buffer [buffer# offset# length#]
         (-> (dtype-proto/sub-buffer ~buffer offset# length#)
             (dtype-proto/->writer {:datatype ~intermediate-datatype
                                    :unchecked? ~unchecked?})))
       dtype-proto/PSetConstant
       (set-constant! [item# offset# value# elem-count#]
         (dtype-proto/set-constant! ~buffer offset#
                                    (casting/cast value# ~intermediate-datatype)
                                    elem-count#)))
     (reify ~writer-type
       (getDatatype [writer#] ~intermediate-datatype)
       (lsize [writer#] (int (mp/element-count ~buffer)))
       (write [writer# idx# value#]
         (cls-type->write-fn ~buffer-type ~buffer idx# ~buffer-pos
                             (checked-full-write-cast value# ~writer-datatype
                                                      ~intermediate-datatype
                                                      ~buffer-datatype)))
       dtype-proto/PToNioBuffer
       (convertible-to-nio-buffer? [writer#]
         (dtype-proto/nio-convertible? ~buffer))
       (->buffer-backing-store [writer#]
         (dtype-proto/as-nio-buffer ~buffer))
       dtype-proto/PToList
       (convertible-to-fastutil-list? [writer#]
         (dtype-proto/list-convertible? ~buffer))
       (->list-backing-store [writer#]
         (dtype-proto/as-list ~buffer))
       jna/PToPtr
       (is-jna-ptr-convertible? [writer#]
         (jna/ptr-convertible? ~buffer))
       (->ptr-backing-store [writer#]
         (jna/as-ptr ~buffer))

       dtype-proto/PToArray
       (->sub-array [reader#]
         (dtype-proto/->sub-array ~buffer))
       (->array-copy [reader#]
         (dtype-proto/->array-copy ~buffer))

       dtype-proto/PBuffer
       (sub-buffer [buffer# offset# length#]
         (-> (dtype-proto/sub-buffer ~buffer offset# length#)
             (dtype-proto/->writer {:datatype ~intermediate-datatype
                                    :unchecked? ~unchecked?})))
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
                   (let [buffer# (typecast/datatype->buffer-cast-fn
                                  ~buffer-datatype buffer#)
                         buffer-pos# (datatype->pos-fn ~buffer-datatype buffer#)]
                     (make-buffer-writer-impl
                      ~(typecast/datatype->writer-type dtype)
                      ~(typecast/datatype->buffer-type buffer-datatype)
                      buffer# buffer-pos#
                      ~(casting/datatype->safe-host-type dtype) ~dtype
                      ~buffer-datatype
                      unchecked?#)))]))]
        (into {})))



(def buffer-writer-table (make-buffer-writer-table))


(defn make-buffer-writer
  [item unchecked?]
  (let [nio-buffer (dtype-proto/->buffer-backing-store item)
        item-dtype (dtype-proto/get-datatype item)
        buffer-dtype (dtype-proto/get-datatype nio-buffer)
        no-translate-writer (get buffer-writer-table [buffer-dtype item-dtype])]
    (no-translate-writer nio-buffer unchecked?)))


(defmacro make-list-writer-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             (let [buffer-datatype (casting/datatype->host-datatype dtype)]
               [[buffer-datatype dtype]
                `(fn [buffer# unchecked?#]
                   (let [buffer# (typecast/datatype->list-cast-fn
                                  ~buffer-datatype
                                                                  buffer#)]
                     (make-buffer-writer-impl
                      ~(typecast/datatype->writer-type dtype)
                      ~(typecast/datatype->list-type buffer-datatype)
                      buffer# 0
                      ~(casting/datatype->safe-host-type dtype) ~dtype
                      ~buffer-datatype
                      unchecked?#)))]))]
        (into {})))


(def list-writer-table (make-list-writer-table))


(defn make-list-writer
  [item unchecked?]
  (let [nio-list (dtype-proto/->list-backing-store item)
        item-dtype (casting/flatten-datatype (dtype-proto/get-datatype item))
        list-dtype (dtype-proto/get-datatype nio-list)
        no-translate-writer (get list-writer-table [list-dtype item-dtype])]
    (no-translate-writer nio-list unchecked?)))



(defmacro make-derived-writer
  ([writer-datatype runtime-datatype options src-writer writer-op create-fn n-elems]
   `(let [src-writer# ~src-writer
          ~'src-writer src-writer#
          n-elems# ~n-elems
          runtime-datatype# ~runtime-datatype
          unchecked?# (:unchecked? ~options)]
      (reify
        ~(typecast/datatype->writer-type writer-datatype)
        (getDatatype [writer#] runtime-datatype#)
        (lsize [writer#] n-elems#)
        (write [writer# ~'idx ~'value]
          ~writer-op)
        dtype-proto/PToBackingStore
        (->backing-store-seq [writer#]
          (dtype-proto/->backing-store-seq src-writer#))
        dtype-proto/PBuffer
        (sub-buffer [writer# offset# length#]
          (-> (dtype-proto/sub-buffer src-writer# offset# length#)
              (~create-fn {:datatype runtime-datatype#
                           :unchecked? unchecked?#}))))))
  ([writer-datatype runtime-datatype options src-writer writer-op create-fn]
   `(make-derived-writer ~writer-datatype ~runtime-datatype ~options
                         ~src-writer ~writer-op ~create-fn (.lsize ~'src-writer))))


(defn- make-object-wrapper
  [writer datatype & [unchecked?]]
  (let [item-dtype (dtype-proto/get-datatype writer)]
    (when-not (and (= :object (casting/flatten-datatype item-dtype))
                   (= :object (casting/flatten-datatype datatype)))
      (throw (ex-info "Incorrect use of object wrapper" {}))))
  (if (= datatype (dtype-proto/get-datatype writer))
    writer
    (let [obj-writer (typecast/datatype->writer :object writer)]
      (make-derived-writer :object datatype true obj-writer
                           (.write obj-writer idx value)
                           make-object-wrapper))))


(declare make-marshalling-writer)


(defmacro make-marshalling-writer-table
  []
  `(->> [~@(for [dtype (casting/all-datatypes)
                 dst-writer-datatype casting/all-host-datatypes]
             (let [writer-dtype (casting/safe-flatten dtype)]
               [[dst-writer-datatype dtype]
                `(fn [dst-writer# options#]
                   (let [dst-writer# (typecast/datatype->writer
                                      ~dst-writer-datatype dst-writer# true)
                         unchecked?# (:unchecked? options#)]
                     (if unchecked?#
                       (make-derived-writer ~writer-dtype ~dtype options# dst-writer#
                                            (.write dst-writer# ~'idx
                                                    (unchecked-full-cast
                                                     ~'value
                                                     ~writer-dtype
                                                     ~dtype
                                                     ~dst-writer-datatype))
                                            dtype-proto/->writer)
                       (make-derived-writer ~writer-dtype ~dtype options# dst-writer#
                                            (.write dst-writer# ~'idx
                                                    (checked-full-read-cast
                                                     ~'value
                                                     ~writer-dtype
                                                     ~dtype
                                                     ~dst-writer-datatype))
                                            dtype-proto/->writer))))]))]
        (into {})))


(def marshalling-writer-table (make-marshalling-writer-table))


(defn make-marshalling-writer
  [dst-writer options]
  (let [dst-dtype (dtype-proto/get-datatype dst-writer)
        src-dtype (or (:datatype options)
                      (dtype-proto/get-datatype dst-writer))]
    (if (= (casting/safe-flatten dst-dtype) (casting/safe-flatten src-dtype))
      dst-writer
      (let [writer-fn (get marshalling-writer-table
                           [(casting/safe-flatten dst-dtype)
                            (casting/flatten-datatype src-dtype)])
            dst-writer (writer-fn dst-writer options)]
        (if (and (= :object (casting/flatten-datatype src-dtype))
                 (not= :object src-dtype))
          (make-object-wrapper dst-writer src-dtype)
          dst-writer)))))



(defmacro extend-writer-type
  [writer-type datatype]
  `(clojure.core/extend
       ~writer-type
     dtype-proto/PToWriter
     {:convertible-to-writer? (fn [item#] true)
      :->writer
      (fn [item# options#]
        (make-marshalling-writer item# options#))}
     dtype-proto/PBuffer
     {:sub-buffer (fn [item# offset# length#]
                    (let [src-writer# (typecast/datatype->writer ~datatype item# true)
                          src-dtype# (dtype-proto/get-datatype src-writer#)
                          ~'offset (int offset#)
                          ~'length (int length#)
                          end-elem# (+ ~'offset ~'length)]
                      (make-derived-writer
                       ~datatype src-dtype# {} src-writer#
                       (do
                         (when-not (< ~'idx ~'length)
                           (throw (ex-info (format "Index out of range: %s > %s" ~'idx
                                                   ~'length)
                                           {})))
                         (.write ~'src-writer (+ ~'idx ~'offset) ~'value))
                       dtype-proto/->writer
                       ~'length)))}))


(extend-writer-type ByteWriter :int8)
(extend-writer-type ShortWriter :int16)
(extend-writer-type IntWriter :int32)
(extend-writer-type LongWriter :int64)
(extend-writer-type FloatWriter :float32)
(extend-writer-type DoubleWriter :float64)
(extend-writer-type BooleanWriter :boolean)
(extend-writer-type ObjectWriter :object)

(declare make-indexed-writer)


(defmacro make-indexed-writer-impl
  [datatype writer-type indexes values unchecked?]
  `(let [idx-reader# (datatype->reader :int32 ~indexes true)
         values# (datatype->writer ~datatype ~values ~unchecked?)
         writer-dtype# (dtype-proto/get-datatype ~values)
         n-elems# (.lsize idx-reader#)]
     (reify
       ~(typecast/datatype->writer-type datatype)
       (getDatatype [writer#] writer-dtype#)
       (lsize [writer#] n-elems#)
       (write [writer# idx# value#]
         (.write values# (.read idx-reader# idx#) value#))
       dtype-proto/PToBackingStore
       (->backing-store-seq [writer#]
         (dtype-proto/->backing-store-seq values#))
       dtype-proto/PBuffer
       (sub-buffer [writer# offset# length#]
         (-> (dtype-proto/sub-buffer ~indexes offset# length#)
             (make-indexed-writer ~values {:unchecked? ~unchecked?}))))))


(defmacro make-indexed-writer-creators
  []
  `(->> [~@(for [dtype (casting/all-datatypes)]
             [dtype `(fn [indexes# values# unchecked?#]
                       (make-indexed-writer-impl
                        ~dtype ~(typecast/datatype->writer-type dtype)
                        indexes# values# unchecked?#))])]
        (into {})))

(def indexed-writer-creators (make-indexed-writer-creators))


(defn make-indexed-writer
  [indexes values {:keys [datatype unchecked?]}]
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
             (.write dst-writer# idx# (typecast/datatype->iter-next-fn
                                       ~datatype src-iter#))
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
