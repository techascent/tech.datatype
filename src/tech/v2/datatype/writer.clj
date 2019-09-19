(ns tech.v2.datatype.writer
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.nio-access
             :refer [datatype->pos-fn
                     unchecked-full-cast
                     checked-full-write-cast
                     cls-type->write-fn]]
            [tech.jna :as jna]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.shape :as dtype-shape])
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
   unchecked?
   src-item]
  `(if ~unchecked?
     (reify
       ~writer-type
       (lsize [writer#] (dtype-shape/ecount ~src-item))
       (write [writer# idx# value#]
         (cls-type->write-fn ~buffer-type ~buffer idx# ~buffer-pos
          (unchecked-full-cast value# ~writer-datatype
                               ~intermediate-datatype
                               ~buffer-datatype)))
       dtype-proto/PToNioBuffer
       (convertible-to-nio-buffer? [writer#]
         (dtype-proto/nio-convertible? ~src-item))
       (->buffer-backing-store [writer#]
         (dtype-proto/as-nio-buffer ~src-item))
       dtype-proto/PToList
       (convertible-to-fastutil-list? [writer#]
         (dtype-proto/list-convertible? ~src-item))
       (->list-backing-store [writer#]
         (dtype-proto/as-list ~src-item))
       jna/PToPtr
       (is-jna-ptr-convertible? [writer#]
         (jna/ptr-convertible? ~src-item))
       (->ptr-backing-store [writer#]
         (jna/as-ptr ~src-item))
       dtype-proto/PToArray
       (->sub-array [reader#]
         (dtype-proto/->sub-array ~src-item))
       (->array-copy [reader#]
         (dtype-proto/->array-copy ~src-item))

       dtype-proto/PBuffer
       (sub-buffer [buffer# offset# length#]
         (-> (dtype-proto/sub-buffer ~src-item offset# length#)
             (dtype-proto/->writer {:datatype ~intermediate-datatype
                                    :unchecked? ~unchecked?})))
       dtype-proto/PSetConstant
       (set-constant! [item# offset# value# elem-count#]
         (dtype-proto/set-constant! ~src-item offset#
                                    (casting/cast value# ~intermediate-datatype)
                                    elem-count#)))
     (reify ~writer-type
       (lsize [writer#] (dtype-shape/ecount ~src-item))
       (write [writer# idx# value#]
         (cls-type->write-fn ~buffer-type ~buffer idx# ~buffer-pos
                             (checked-full-write-cast value# ~writer-datatype
                                                      ~intermediate-datatype
                                                      ~buffer-datatype)))
       dtype-proto/PToNioBuffer
       (convertible-to-nio-buffer? [writer#]
         (dtype-proto/nio-convertible? ~src-item))
       (->buffer-backing-store [writer#]
         (dtype-proto/as-nio-buffer ~src-item))
       dtype-proto/PToList
       (convertible-to-fastutil-list? [writer#]
         (dtype-proto/list-convertible? ~src-item))
       (->list-backing-store [writer#]
         (dtype-proto/as-list ~src-item))
       jna/PToPtr
       (is-jna-ptr-convertible? [writer#]
         (jna/ptr-convertible? ~src-item))
       (->ptr-backing-store [writer#]
         (jna/as-ptr ~src-item))

       dtype-proto/PToArray
       (->sub-array [reader#]
         (dtype-proto/->sub-array ~src-item))
       (->array-copy [reader#]
         (dtype-proto/->array-copy ~src-item))

       dtype-proto/PBuffer
       (sub-buffer [buffer# offset# length#]
         (-> (dtype-proto/sub-buffer ~src-item offset# length#)
             (dtype-proto/->writer {:datatype ~intermediate-datatype
                                    :unchecked? ~unchecked?})))
       dtype-proto/PSetConstant
       (set-constant! [item# offset# value# elem-count#]
         (dtype-proto/set-constant! ~src-item offset#
                                    (casting/cast value# ~intermediate-datatype)
                                    elem-count#)))))


(defmacro make-buffer-writer-table
  []
  `(->> [~@(for [{:keys [intermediate-datatype
                         reader-datatype
                         buffer-datatype]
                  :as access-map}
                 (->> casting/buffer-access-table
                      (filter (comp casting/numeric-type?
                                    :intermediate-datatype)))]
             (let [writer-datatype reader-datatype]
               [access-map
                `(fn [src-item# buffer# unchecked?#]
                   (let [buffer# (typecast/datatype->buffer-cast-fn
                                  ~buffer-datatype buffer#)
                         buffer-pos# (datatype->pos-fn ~buffer-datatype buffer#)]
                     (make-buffer-writer-impl
                      ~(typecast/datatype->writer-type writer-datatype)
                      ~(typecast/datatype->buffer-type buffer-datatype)
                      buffer# buffer-pos#
                      ~writer-datatype
                      ~intermediate-datatype
                      ~buffer-datatype
                      unchecked?#
                      src-item#)))]))]
        (into {})))



(def buffer-writer-table (make-buffer-writer-table))


(defn make-buffer-writer
  [item writer-datatype intermediate-datatype & [unchecked?]]
  (let [nio-buffer (dtype-proto/->buffer-backing-store item)
        buffer-dtype (dtype-proto/get-datatype nio-buffer)
        access-map {:intermediate-datatype intermediate-datatype
                    :reader-datatype writer-datatype
                    :buffer-datatype buffer-dtype}
        no-translate-writer (get buffer-writer-table access-map)]
    (when-not no-translate-writer
      (throw (ex-info "Failed to find writer for buffer and datatype combination"
                      access-map)))
    (no-translate-writer item nio-buffer unchecked?)))


(defmacro make-list-writer-table
  []
  `(->> [~@(for [{:keys [intermediate-datatype
                         reader-datatype
                         buffer-datatype]
                  :as access-map} casting/buffer-access-table]
             (let [writer-datatype reader-datatype]
               [access-map
                `(fn [src-item# buffer# unchecked?#]
                   (let [buffer# (typecast/datatype->list-cast-fn
                                  ~buffer-datatype
                                  buffer#)]
                     (make-buffer-writer-impl
                      ~(typecast/datatype->writer-type writer-datatype)
                      ~(typecast/datatype->list-type buffer-datatype)
                      buffer# 0
                      ~writer-datatype
                      ~intermediate-datatype
                      ~buffer-datatype
                      unchecked?#
                      src-item#)))]))]
        (into {})))


(def list-writer-table (make-list-writer-table))


(defn make-list-writer
  [item writer-datatype intermediate-datatype & [unchecked?]]
  (let [nio-list (dtype-proto/->list-backing-store item)
        buffer-dtype (dtype-proto/get-datatype nio-list)
        access-map {:intermediate-datatype (casting/flatten-datatype intermediate-datatype)
                    :reader-datatype writer-datatype
                    :buffer-datatype buffer-dtype}
        no-translate-writer (get list-writer-table access-map)]
    (when-not no-translate-writer
      (throw (ex-info "Failed to find writer for buffer and datatype combination"
                      access-map)))
    (no-translate-writer item nio-list unchecked?)))



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
  [writer datatype]
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


(defmacro make-marshalling-writer-macro
  [src-dtype dst-dtype]
  `(fn [dst-writer# datatype# options#]
     (let [dst-writer# (typecast/datatype->writer
                        ~dst-dtype dst-writer# true)
           unchecked?# (:unchecked? options#)]
       (if unchecked?#
         (make-derived-writer ~src-dtype datatype# options# dst-writer#
                              (.write dst-writer# ~'idx
                                      (casting/datatype->unchecked-cast-fn
                                       ~src-dtype
                                       ~dst-dtype
                                       ~'value))
                              dtype-proto/->writer)
         (make-derived-writer ~src-dtype datatype# options# dst-writer#
                              (.write dst-writer# ~'idx
                                      (casting/datatype->cast-fn
                                       ~src-dtype
                                       ~dst-dtype
                                       ~'value))
                              dtype-proto/->writer)))))


(def marshalling-writer-table (casting/make-marshalling-item-table
                               make-marshalling-writer-macro))


(defn make-marshalling-writer
  [dst-writer options]
  (let [dst-dtype (casting/safe-flatten
                   (dtype-proto/get-datatype dst-writer))
        src-dtype (casting/safe-flatten
                   (or (:datatype options)
                       (dtype-proto/get-datatype dst-writer)))]
    (if (= dst-dtype src-dtype)
      dst-writer
      (let [writer-fn (get marshalling-writer-table
                           [src-dtype dst-dtype])]
        (when-not writer-fn
          (throw (ex-info (format "Failed to create marshalling writer %s->%s"
                                  src-dtype dst-dtype)
                          {})))
        (let [retval-writer (writer-fn dst-writer src-dtype options)
              retval-dtype (dtype-proto/get-datatype retval-writer)]
          (if-not (= retval-dtype src-dtype)
            (make-object-wrapper retval-writer src-dtype)
            retval-writer))))))


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



(defmacro typed-write
  [datatype item idx value]
  `(.write (typecast/datatype->writer ~datatype ~item)
           ~idx ~value))
