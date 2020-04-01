(ns tech.v2.datatype.reader
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.jna :as jna]
            [tech.v2.datatype.nio-access
             :refer [datatype->pos-fn
                     unchecked-full-cast
                     checked-full-write-cast
                     cls-type->read-fn]]
            [tech.v2.datatype.typecast :as typecast]
            ;;Load all iterator bindings
            [tech.v2.datatype.iterator]
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
  [reader-datatype intermediate-datatype buffer-datatype buffer-type
   unchecked? src-item buffer buffer-pos
   advertised-datatype]
  `(let [src-item# ~src-item
         buffer# ~buffer
         buffer-pos# ~buffer-pos
         n-elems# (ecount src-item#)
         unchecked?# ~unchecked?]
     (if unchecked?#
       (reify
         ~(typecast/datatype->reader-type reader-datatype)
         (getDatatype [reader#] ~advertised-datatype)
         (lsize [reader#] n-elems#)
         (read [reader# idx#]
           (-> (cls-type->read-fn ~buffer-type ~buffer-datatype buffer#
                                  idx# buffer-pos#)
               (unchecked-full-cast ~buffer-datatype ~intermediate-datatype
                                    ~reader-datatype)))
         dtype-proto/PToBackingStore
         (->backing-store-seq [reader#]
           (dtype-proto/->backing-store-seq src-item#))
         dtype-proto/PToNioBuffer
         (convertible-to-nio-buffer? [reader#]
           (dtype-proto/nio-convertible? src-item#))
         (->buffer-backing-store [reader#]
           (dtype-proto/as-nio-buffer src-item#))
         dtype-proto/PToList
         (convertible-to-fastutil-list? [reader#]
           (dtype-proto/list-convertible? src-item#))
         (->list-backing-store [reader#]
           (dtype-proto/as-list src-item#))
         dtype-proto/PToJNAPointer
         (convertible-to-data-ptr? [reader#]
           (dtype-proto/convertible-to-data-ptr? src-item#))
         (->jna-ptr [reader#]
           (dtype-proto/->jna-ptr src-item#))

         dtype-proto/PToArray
         (->sub-array [reader#]
           (dtype-proto/->sub-array src-item#))
         (->array-copy [reader#]
           (dtype-proto/->array-copy src-item#))

         dtype-proto/PClone
         (clone [reader#]
           (-> (dtype-proto/clone buffer#)
               (dtype-proto/->reader {:datatype ~intermediate-datatype
                                      :unchecked? unchecked?#})))
         dtype-proto/PBuffer
         (sub-buffer [buffer# offset# length#]
           (-> (dtype-proto/sub-buffer src-item# offset# length#)
               (dtype-proto/->reader {:datatype ~intermediate-datatype
                                      :unchecked? unchecked?#})))
         dtype-proto/PSetConstant
         (set-constant! [item# offset# value# elem-count#]
           (dtype-proto/set-constant! src-item# offset#
                                      (casting/cast value# ~intermediate-datatype)
                                      elem-count#))
         dtype-proto/PConvertibleToBinaryReader
         (convertible-to-binary-reader? [rdr#]
           (dtype-proto/convertible-to-binary-reader? buffer#))
         (->binary-reader [rdr# options#]
           (dtype-proto/->binary-reader buffer# options#)))
       (reify
         ~(typecast/datatype->reader-type reader-datatype)
         (getDatatype [reader#] ~advertised-datatype)
         (lsize [reader#] n-elems#)
         (read [reader# idx#]
           (-> (cls-type->read-fn ~buffer-type ~buffer-datatype
                                  buffer# idx# buffer-pos#)
               (checked-full-write-cast ~buffer-datatype ~intermediate-datatype
                                        ~reader-datatype)))
         dtype-proto/PToBackingStore
         (->backing-store-seq [reader#]
           (dtype-proto/->backing-store-seq src-item#))
         dtype-proto/PToNioBuffer
         (convertible-to-nio-buffer? [reader#]
           (dtype-proto/nio-convertible? src-item#))
         (->buffer-backing-store [reader#]
           (dtype-proto/as-nio-buffer src-item#))
         dtype-proto/PToList
         (convertible-to-fastutil-list? [reader#]
           (dtype-proto/list-convertible? src-item#))
         (->list-backing-store [reader#]
           (dtype-proto/as-list src-item#))
         dtype-proto/PToJNAPointer
         (convertible-to-data-ptr? [reader#]
           (dtype-proto/convertible-to-data-ptr? src-item#))
         (->jna-ptr [reader#]
           (dtype-proto/->jna-ptr src-item#))

         dtype-proto/PToArray
         (->sub-array [reader#]
           (dtype-proto/->sub-array src-item#))
         (->array-copy [reader#]
           (dtype-proto/->array-copy src-item#))

         dtype-proto/PBuffer
         (sub-buffer [reader# offset# length#]
           (-> (dtype-proto/sub-buffer src-item# offset# length#)
               (dtype-proto/->reader {:datatype ~intermediate-datatype
                                      :unchecked? unchecked?#})))
         dtype-proto/PSetConstant
         (set-constant! [reader# offset# value# elem-count#]
           (dtype-proto/set-constant! src-item# offset#
                                      (casting/cast value# ~intermediate-datatype)
                                      elem-count#))
         dtype-proto/PConvertibleToBinaryReader
         (convertible-to-binary-reader? [rdr#]
           (dtype-proto/convertible-to-binary-reader? buffer#))
         (->binary-reader [rdr# options#]
           (dtype-proto/->binary-reader buffer# options#))))))


(defmacro make-buffer-reader-table
  []
  `(->> [~@(for [{:keys [intermediate-datatype
                         buffer-datatype
                         reader-datatype]
                  :as access-map}
                 (->> casting/buffer-access-table
                      (filter (comp casting/numeric-types :intermediate-datatype)))]
             [access-map
              `(fn [src-item# buffer# unchecked?# advertised-dtype#]
                 (let [buffer# (typecast/datatype->buffer-cast-fn ~buffer-datatype
                                                                  buffer#)
                       buffer-pos# (datatype->pos-fn ~buffer-datatype buffer#)]
                   (if (== 0 buffer-pos#)
                     (make-buffer-reader-impl ~reader-datatype ~intermediate-datatype
                                              ~buffer-datatype
                                              ~(typecast/datatype->buffer-type
                                                buffer-datatype)
                                              unchecked?# src-item#
                                              buffer# 0
                                              advertised-dtype#)
                     (make-buffer-reader-impl ~reader-datatype ~intermediate-datatype
                                              ~buffer-datatype
                                              ~(typecast/datatype->buffer-type
                                                buffer-datatype)
                                              unchecked?# src-item#
                                              buffer# buffer-pos#
                                              advertised-dtype#))))])]
        (into {})))


(def buffer-reader-table (make-buffer-reader-table))


(defn make-buffer-reader
  [item
   reader-datatype
   intermediate-datatype
   & [unchecked?]]
  (let [nio-buffer (dtype-proto/->buffer-backing-store item)
        buffer-dtype (dtype-proto/get-datatype nio-buffer)
        ;;There could be aliased datatypes passed into here
        access-key-int-dtype (casting/flatten-datatype intermediate-datatype)
        access-map {:reader-datatype reader-datatype
                    :intermediate-datatype access-key-int-dtype
                    :buffer-datatype buffer-dtype}
        buffer-reader-fn (get buffer-reader-table access-map)]
    (when-not buffer-reader-fn
      (throw (ex-info "Failed to find nio reader creation function for buffer datatype"
                      access-map)))
    (buffer-reader-fn item nio-buffer unchecked? intermediate-datatype)))


(defmacro make-list-reader-table
  []
  `(->> [~@(for [{:keys [intermediate-datatype
                         buffer-datatype
                         reader-datatype]
                  :as access-map} casting/buffer-access-table]
             [access-map `(fn [src-item# buffer# unchecked?# advertised-datatype#]
                            (let [buffer# (typecast/datatype->list-cast-fn ~buffer-datatype
                                                                           buffer#)
                                  buffer-pos# 0]
                              (make-buffer-reader-impl ~reader-datatype ~intermediate-datatype
                                                       ~buffer-datatype
                                                       ~(typecast/datatype->list-type buffer-datatype)
                                                       unchecked?# src-item#
                                                       buffer# buffer-pos#
                                                       advertised-datatype#)))])]
        (into {})))


(def list-reader-table (make-list-reader-table))


(defn make-list-reader
  [item
   reader-datatype
   intermediate-datatype
   & [unchecked?]]
  (let [list-buffer (dtype-proto/->list-backing-store item)
        buffer-dtype (dtype-proto/get-datatype list-buffer)
        access-map {:reader-datatype reader-datatype
                    :intermediate-datatype (casting/flatten-datatype intermediate-datatype)
                    :buffer-datatype buffer-dtype}
        list-reader-fn (get list-reader-table access-map)]
    (when-not list-reader-fn
      (throw (ex-info "Failed to find reader creation function for buffer datatype"
                      access-map)))
    (list-reader-fn item list-buffer unchecked? intermediate-datatype)))


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
  ([reader datatype options]
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
  ([reader options]
   (make-object-wrapper reader (:datatype options) options)))


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



(def marshalling-reader-table (casting/make-marshalling-item-table
                               make-marshalling-reader-macro))


(defn make-marshalling-reader
  [src-reader options]
  (let [src-dtype (dtype-proto/get-datatype src-reader)
        dst-dtype (or (:datatype options)
                      (dtype-proto/get-datatype src-reader))]
    (if (= src-dtype dst-dtype)
      src-reader
      (let [reader-fn (get marshalling-reader-table
                           [(casting/safe-flatten src-dtype)
                            (casting/safe-flatten dst-dtype)])]
        (reader-fn src-reader dst-dtype options)))))


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
     dtype-proto/PClone
     {:clone
      (fn [item#]
        (let [item-dtype# (dtype-proto/get-datatype item#)]
          (if (= ~datatype item-dtype#)
            (dtype-proto/make-container :java-array ~datatype item# {})
            (dtype-proto/make-container :typed-buffer item-dtype# item# {}))))}
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
