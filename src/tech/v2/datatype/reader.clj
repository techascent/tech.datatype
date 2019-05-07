(ns tech.v2.datatype.reader
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.jna :as jna]
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
            [tech.v2.datatype.typecast :refer :all :as typecast]
            ;;Load all iterator bindings
            [tech.v2.datatype.iterator]
            [tech.v2.datatype.argtypes :as argtypes]
            [tech.v2.datatype.shape
             :refer [ecount]
             :as dtype-shape])
  (:import [tech.v2.datatype ObjectReader ObjectReaderIter ObjectIter
            ByteReader ByteReaderIter ByteIter
            ShortReader ShortReaderIter ShortIter
            IntReader IntReaderIter IntIter
            LongReader LongReaderIter LongIter
            FloatReader FloatReaderIter FloatIter
            DoubleReader DoubleReaderIter DoubleIter
            BooleanReader BooleanReaderIter BooleanIter]
           [java.nio Buffer ByteBuffer ShortBuffer
            IntBuffer LongBuffer FloatBuffer DoubleBuffer]
           [it.unimi.dsi.fastutil.bytes ByteList ByteArrayList]
           [it.unimi.dsi.fastutil.shorts ShortList ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntList IntArrayList]
           [it.unimi.dsi.fastutil.longs LongList LongArrayList]
           [it.unimi.dsi.fastutil.floats FloatList FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleList DoubleArrayList]
           [it.unimi.dsi.fastutil.booleans BooleanList BooleanArrayList]
           [it.unimi.dsi.fastutil.objects ObjectList ObjectArrayList]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro make-buffer-reader-impl
  [reader-type buffer-type buffer buffer-pos
   reader-datatype
   intermediate-datatype
   buffer-datatype
   unchecked?]
  `(let [n-elems# (int (ecount ~buffer))]
     (if ~unchecked?
       (reify
         ~reader-type
         (getDatatype [reader#] ~intermediate-datatype)
         (lsize [reader#] n-elems#)
         (read [reader# idx#]
           (-> (cls-type->read-fn ~buffer-type ~buffer-datatype ~buffer idx# ~buffer-pos)
               (unchecked-full-cast ~buffer-datatype ~intermediate-datatype
                                    ~reader-datatype)))

         dtype-proto/PToBackingStore
         (->backing-store-seq [item#]
           (dtype-proto/->backing-store-seq ~buffer))
         dtype-proto/PToNioBuffer
         (convertible-to-nio-buffer? [reader#]
           (dtype-proto/nio-convertible? ~buffer))
         (->buffer-backing-store [reader#]
           (dtype-proto/as-nio-buffer ~buffer))
         dtype-proto/PToList
         (convertible-to-fastutil-list? [reader#]
           (dtype-proto/list-convertible? ~buffer))
         (->list-backing-store [reader#]
           (dtype-proto/as-list ~buffer))
         jna/PToPtr
         (is-jna-ptr-convertible? [reader#]
           (jna/ptr-convertible? ~buffer))
         (->ptr-backing-store [reader#]
           (jna/as-ptr ~buffer))

         dtype-proto/PToArray
         (->sub-array [reader#]
           (dtype-proto/->sub-array ~buffer))
         (->array-copy [reader#]
           (dtype-proto/->array-copy ~buffer))

         dtype-proto/PBuffer
         (sub-buffer [buffer# offset# length#]
           (-> (dtype-proto/sub-buffer ~buffer offset# length#)
               (dtype-proto/->reader {:datatype~intermediate-datatype
                                      :unchecked? ~unchecked?})))
         dtype-proto/PSetConstant
         (set-constant! [item# offset# value# elem-count#]
           (dtype-proto/set-constant! ~buffer offset#
                                      (casting/cast value# ~intermediate-datatype)
                                      elem-count#)))
       (reify
         ~reader-type
         (getDatatype [reader#] ~intermediate-datatype)
         (lsize [reader#] n-elems#)
         (read [reader# idx#]
           (-> (cls-type->read-fn ~buffer-type ~buffer-datatype ~buffer idx# ~buffer-pos)
               (checked-full-write-cast ~buffer-datatype ~intermediate-datatype
                                        ~reader-datatype)))
         (iterator [reader#] (reader->iterator reader#))
         (invoke [reader# arg#]
           (.read reader# (int arg#)))
         dtype-proto/PToBackingStore
         (->backing-store-seq [item#]
           (dtype-proto/->backing-store-seq ~buffer))
         dtype-proto/PToNioBuffer
         (convertible-to-nio-buffer? [reader#]
           (dtype-proto/nio-convertible? ~buffer))
         (->buffer-backing-store [reader#]
           (dtype-proto/as-nio-buffer ~buffer))
         dtype-proto/PToList
         (convertible-to-fastutil-list? [reader#]
           (dtype-proto/list-convertible? ~buffer))
         (->list-backing-store [reader#]
           (dtype-proto/as-list ~buffer))
         jna/PToPtr
         (is-jna-ptr-convertible? [reader#]
           (jna/ptr-convertible? ~buffer))
         (->ptr-backing-store [reader#]
           (jna/as-ptr ~buffer))

         dtype-proto/PToArray
         (->sub-array [reader#]
           (dtype-proto/->sub-array ~buffer))
         (->array-copy [reader#]
           (dtype-proto/->array-copy ~buffer))

         dtype-proto/PBuffer
         (sub-buffer [buffer# offset# length#]
           (-> (dtype-proto/sub-buffer ~buffer offset# length#)
               (dtype-proto/->reader {:datatype ~intermediate-datatype
                                      :unchecked? ~unchecked?})))
         dtype-proto/PSetConstant
         (set-constant! [item# offset# value# elem-count#]
           (dtype-proto/set-constant! ~buffer offset#
                                      (casting/cast value# ~intermediate-datatype)
                                      elem-count#))))))


(defmacro make-buffer-reader-table
  []
  `(->> [~@(for [intermediate-datatype casting/numeric-types]
             (let [buffer-datatype (casting/datatype->host-datatype intermediate-datatype)
                   reader-datatype (casting/safe-flatten intermediate-datatype)]
               [[buffer-datatype intermediate-datatype]
                `(fn [buffer# unchecked?#]
                   (let [buffer# (typecast/datatype->buffer-cast-fn ~buffer-datatype
                                                                    buffer#)
                         buffer-pos# (datatype->pos-fn ~buffer-datatype buffer#)]
                     (make-buffer-reader-impl
                      ~(typecast/datatype->reader-type reader-datatype)
                      ~(typecast/datatype->buffer-type buffer-datatype)
                      buffer# buffer-pos#
                      ~reader-datatype
                      ~intermediate-datatype
                      ~buffer-datatype
                      unchecked?#)))]))]
        (into {})))


(def buffer-reader-table (make-buffer-reader-table))


(defn make-buffer-reader
  [item & [unchecked?]]
  (let [nio-buffer (dtype-proto/->buffer-backing-store item)
        item-dtype (dtype-proto/get-datatype item)
        buffer-dtype (dtype-proto/get-datatype nio-buffer)
        buffer-reader-fn (get buffer-reader-table [buffer-dtype item-dtype])]
    (when-not buffer-reader-fn
      (throw (ex-info "Failed to find reader creation function for buffer datatype"
                      {:buffer-datatype buffer-dtype
                       :item-datatype item-dtype})))
    (buffer-reader-fn nio-buffer unchecked?)))


(defmacro make-list-reader-table
  []
  `(->> [~@(for [intermediate-datatype casting/base-datatypes]
             (let [buffer-datatype (casting/datatype->host-datatype intermediate-datatype)
                   reader-datatype (casting/safe-flatten intermediate-datatype)]
               [[buffer-datatype intermediate-datatype]
                `(fn [buffer# unchecked?#]
                   (let [buffer# (typecast/datatype->list-cast-fn
                                  ~buffer-datatype buffer#)]
                     (make-buffer-reader-impl
                      ~(typecast/datatype->reader-type reader-datatype)
                      ~(typecast/datatype->list-type buffer-datatype)
                      buffer# 0
                      ~reader-datatype
                      ~intermediate-datatype
                      ~buffer-datatype
                      unchecked?#)))]))]
        (into {})))


(def list-reader-table (make-list-reader-table))


(defn make-list-reader
  [item & [unchecked?]]
  (let [list-buffer (dtype-proto/->list-backing-store item)
        item-dtype (casting/flatten-datatype (dtype-proto/get-datatype item))
        buffer-dtype (dtype-proto/get-datatype list-buffer)
        list-reader-fn (or (get list-reader-table
                                  [buffer-dtype (casting/flatten-datatype item-dtype)])
                           (get buffer-reader-table [buffer-dtype buffer-dtype]))]
    (when-not list-reader-fn
      (throw (ex-info "Failed to find reader creation function for buffer datatype"
                      {:buffer-datatype buffer-dtype
                       :item-datatype item-dtype})))
    (list-reader-fn list-buffer unchecked?)))


(defmacro make-derived-reader
  ([reader-datatype runtime-datatype options src-reader reader-op create-fn n-elems]
   `(let [src-reader# ~src-reader
          ~'src-reader src-reader#
          n-elems# ~n-elems
          runtime-datatype# ~runtime-datatype
          unchecked?# (:unchecked? ~options)]
      (reify
        ~(typecast/datatype->reader-type reader-datatype)
        (getDatatype [reader#] runtime-datatype#)
        (lsize [reader#] n-elems#)
        (read [reader# ~'idx]
          ~reader-op)
        dtype-proto/PToBackingStore
        (->backing-store-seq [reader#]
          (dtype-proto/->backing-store-seq src-reader#))
        dtype-proto/PBuffer
        (sub-buffer [reader# offset# length#]
          (-> (dtype-proto/sub-buffer src-reader# offset# length#)
              (~create-fn (assoc ~options
                                 :datatype
                                 runtime-datatype#)))))))
  ([reader-datatype runtime-datatype options src-reader reader-op create-fn]
   `(make-derived-reader ~reader-datatype ~runtime-datatype ~options
                         ~src-reader ~reader-op ~create-fn (.lsize ~'src-reader))))


(defn- make-object-wrapper
  [reader datatype options]
  (let [item-dtype (dtype-proto/get-datatype reader)]
    (when-not (and (= :object (casting/flatten-datatype item-dtype))
                   (= :object (casting/flatten-datatype datatype)))
      (throw (ex-info "Incorrect use of object wrapper"
                      {:item-datatype item-dtype
                       :target-datatype datatype})))
    (if (= datatype item-dtype)
      reader
      (let [obj-reader (typecast/datatype->reader :object reader)]
        (make-derived-reader :object datatype options obj-reader
                             (.read src-reader idx) make-object-wrapper)))))


(declare make-marshalling-reader)


(defmacro make-marshalling-reader-macro
  [src-dtype dst-dtype]
  `(fn [src-reader# datatype# options#]
     (let [src-reader# (typecast/datatype->reader ~src-dtype
                                                  src-reader# true)
           unchecked?# (:unchecked? options#)]
       (if unchecked?#
         (make-derived-reader ~dst-dtype datatype# options# src-reader#
                              (let [value# (.read ~'src-reader ~'idx)]
                                (casting/datatype->unchecked-cast-fn
                                 ~src-dtype
                                 ~dst-dtype
                                 value#))
                              make-marshalling-reader)
         (make-derived-reader ~dst-dtype datatype# options# src-reader#
                              (let [value# (.read ~'src-reader ~'idx)]
                                (casting/datatype->cast-fn
                                 ~src-dtype
                                 ~dst-dtype
                                 value#))
                              make-marshalling-reader)))))



(def marshalling-reader-table (casting/make-marshalling-item-table make-marshalling-reader-macro))


(defn make-marshalling-reader
  [src-reader options]
  (let [src-dtype (casting/safe-flatten
                   (dtype-proto/get-datatype src-reader))
        real-dest-dtype (or (:datatype options)
                            (dtype-proto/get-datatype src-reader))
        dest-dtype (casting/safe-flatten real-dest-dtype)]
    (if (= src-dtype dest-dtype)
      src-reader
      (let [src-reader (let [reader-fn (get marshalling-reader-table
                                            [src-dtype dest-dtype])]
                         (reader-fn src-reader real-dest-dtype
                                    {:unchecked? (:unchecked? options)}))
            src-dtype (dtype-proto/get-datatype src-reader)]
        (if (not= src-dtype dest-dtype)
          (make-object-wrapper src-reader dest-dtype {:unchecked? true})
          src-reader)))))


(defmacro extend-reader-type
  [reader-type datatype]
  `(clojure.core/extend
       ~reader-type
     dtype-proto/PToIterable
     {:convertible-to-iterable? (fn [item#] true)
      :->iterable
      (fn [item# options#]
        (dtype-proto/->reader item# options#))}
     dtype-proto/PToReader
     {:convertible-to-reader? (fn [item#] true)
      :->reader
      (fn [item# options#]
        (make-marshalling-reader item# options#))}
     dtype-proto/PBuffer
     {:sub-buffer (fn [item# offset# length#]
                    (let [src-reader# (typecast/datatype->reader ~datatype item# true)
                          src-dtype# (dtype-proto/get-datatype src-reader#)
                          ~'offset (int offset#)
                          ~'length (int length#)
                          end-elem# (+ ~'offset ~'length)]
                      (make-derived-reader
                       ~datatype src-dtype# {} src-reader#
                       (do
                         (when-not (< ~'idx ~'length)
                           (throw (ex-info (format "Index out of range: %s > %s" ~'idx
                                                   ~'length)
                                           {})))
                         (.read ~'src-reader (+ ~'idx ~'offset)))
                       dtype-proto/->reader
                       ~'length)))}))


(extend-reader-type ByteReader :int8)
(extend-reader-type ShortReader :int16)
(extend-reader-type IntReader :int32)
(extend-reader-type LongReader :int64)
(extend-reader-type FloatReader :float32)
(extend-reader-type DoubleReader :float64)
(extend-reader-type BooleanReader :boolean)
(extend-reader-type ObjectReader :object)


(defmacro typed-read
  [datatype item idx]
  `(.read (typecast/datatype->reader ~datatype ~item)
          ~idx))
