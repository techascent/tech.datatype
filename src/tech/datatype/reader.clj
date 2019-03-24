(ns tech.datatype.reader
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting :as casting]
            [tech.parallel :as parallel]
            [tech.jna :as jna]
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
            [clojure.core.matrix.protocols :as mp]
            [clojure.core.matrix :as m]
            [tech.datatype.typecast :refer :all]
            [tech.datatype.fast-copy :as fast-copy]
            [clojure.core.matrix.protocols :as mp]
            [tech.datatype.io :as dtype-io]
            [tech.datatype.typecast :as typecast]
            ;;Load all iterator bindings
            [tech.datatype.iterator])
  (:import [tech.datatype ObjectReader ObjectReaderIter
            ByteReader ByteReaderIter
            ShortReader ShortReaderIter
            IntReader IntReaderIter
            LongReader LongReaderIter
            FloatReader FloatReaderIter
            DoubleReader DoubleReaderIter
            BooleanReader BooleanReaderIter]
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

(defn ecount
  "Type hinted ecount."
  ^long [item]
  (m/ecount item))


(defmacro make-buffer-reader
  [reader-type buffer-type buffer buffer-pos
   reader-datatype
   intermediate-datatype
   buffer-datatype
   unchecked?]
  `(if ~unchecked?
     (reify ~reader-type
       (getDatatype [reader#] ~intermediate-datatype)
       (size [reader#] (int (mp/element-count ~buffer)))
       (read [reader# idx#]
         (-> (cls-type->read-fn ~buffer-type ~buffer-datatype ~buffer idx# ~buffer-pos)
             (unchecked-full-cast ~buffer-datatype ~intermediate-datatype
                                  ~reader-datatype)))
       (iterator [reader#] (reader->iterator reader#)))
     (reify ~reader-type
       (getDatatype [reader#] ~intermediate-datatype)
       (size [reader#] (int (mp/element-count ~buffer)))
       (read [reader# idx#]
         (-> (cls-type->read-fn ~buffer-type ~buffer-datatype ~buffer idx# ~buffer-pos)
             (checked-full-write-cast ~buffer-datatype ~intermediate-datatype
                                      ~reader-datatype)))
       (iterator [reader#] (reader->iterator reader#)))))


(defmacro make-buffer-reader-table
  []
  `(->> [~@(for [dtype casting/base-datatypes
                 buffer-datatype casting/host-numeric-types]
            [[buffer-datatype dtype]
             `(fn [buffer# unchecked?#]
                (let [buffer# (typecast/datatype->buffer-cast-fn ~buffer-datatype
                                                                 buffer#)
                      buffer-pos# (datatype->pos-fn ~buffer-datatype buffer#)]
                  (make-buffer-reader
                   ~(typecast/datatype->reader-type dtype)
                   ~(typecast/datatype->buffer-type buffer-datatype)
                   buffer# buffer-pos#
                   ~(casting/datatype->safe-host-type dtype) ~dtype
                   ~buffer-datatype
                   unchecked?#)))])]
        (into {})))


(def buffer-reader-table (make-buffer-reader-table))


(defmacro make-list-reader-table
  []
  `(->> [~@(for [dtype casting/base-datatypes
                 buffer-datatype casting/all-host-datatypes]
            [[buffer-datatype dtype]
             `(fn [buffer# unchecked?#]
                (let [buffer# (typecast/datatype->list-cast-fn ~buffer-datatype buffer#)]
                  (make-buffer-reader
                   ~(typecast/datatype->reader-type dtype)
                   ~(typecast/datatype->list-type buffer-datatype)
                   buffer# 0
                   ~(casting/datatype->safe-host-type dtype) ~dtype
                   ~buffer-datatype
                   unchecked?#)))])]
        (into {})))


(def list-reader-table (make-list-reader-table))



(defmacro make-marshalling-reader
  [src-reader src-dtype intermediate-dtype result-dtype dst-reader-type unchecked?]
  `(if ~unchecked?
     (reify ~dst-reader-type
       (getDatatype [reader#] ~intermediate-dtype)
       (size [reader#] (.size ~src-reader))
       (read [item# idx#]
         (-> (.read ~src-reader idx#)
             (unchecked-full-cast ~src-dtype ~intermediate-dtype ~result-dtype)))
       (iterator [reader#] (reader->iterator reader#)))
     (reify ~dst-reader-type
       (getDatatype [reader#] ~intermediate-dtype)
       (size [reader#] (.size ~src-reader))
       (read [item# idx#]
         (-> (.read ~src-reader idx#)
             (checked-full-write-cast ~src-dtype ~intermediate-dtype ~result-dtype)))
       (iterator [reader#] (reader->iterator reader#)))))


(defmacro make-marshalling-reader-table
  []
  `(->> [~@(for [dtype (casting/all-datatypes)
                 src-reader-datatype casting/all-host-datatypes]
            [[src-reader-datatype dtype]
             `(fn [src-reader# unchecked?#]
                (let [src-reader# (typecast/datatype->reader ~src-reader-datatype
                                                             src-reader# true)]
                  (make-marshalling-reader
                   src-reader#
                   ~src-reader-datatype
                   ~dtype
                   ~(casting/datatype->safe-host-type dtype)
                   ~(typecast/datatype->reader-type
                     (casting/datatype->safe-host-type dtype))
                   unchecked?#)))])]
        (into {})))



(def marshalling-reader-table (make-marshalling-reader-table))


(defmacro extend-reader-type
  [reader-type datatype]
  `(clojure.core/extend
       ~reader-type
     dtype-proto/PToIterator
     {:->iterator-of-type
      (fn [item# dtype# unchecked?#]
        (.iterator ^Iterable (dtype-proto/->reader-of-type
                              item# dtype# unchecked?#)))}
     dtype-proto/PToReader
     {:->reader-of-type
      (fn [item# dtype# unchecked?#]
        (if (= dtype# (dtype-proto/get-datatype item#))
          item#
          (if-let [reader-fn# (get marshalling-reader-table
                                   [~datatype (casting/flatten-datatype dtype#)])]
            (reader-fn# item# unchecked?#)
            (throw (ex-info (format "Failed to find marshalling reader %s->%s"
                                    ~datatype dtype#) {})))))}))


(extend-reader-type ByteReader :int8)
(extend-reader-type ShortReader :int16)
(extend-reader-type IntReader :int32)
(extend-reader-type LongReader :int64)
(extend-reader-type FloatReader :float32)
(extend-reader-type DoubleReader :float64)
(extend-reader-type BooleanReader :boolean)
(extend-reader-type ObjectReader :object)


(defmacro make-const-reader
  [datatype]
  `(fn [item# num-elems#]
     (let [num-elems# (int (or num-elems# Integer/MAX_VALUE))
           item# (checked-full-write-cast
                  item# :unknown ~datatype
                  ~(casting/datatype->safe-host-type datatype))]
       (reify ~(typecast/datatype->reader-type datatype)
         (getDatatype [reader#] ~datatype)
         (size [reader#] num-elems#)
         (read [reader# idx#] item#)
         (iterator [reader#] (typecast/reader->iterator reader#))))))

(defmacro make-const-reader-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             [dtype `(make-const-reader ~dtype)])]
        (into {})))


(def const-reader-table (make-const-reader-table))


(defn make-const-reader
  [item datatype & [num-elems]]
  (if-let [reader-fn (get const-reader-table (casting/flatten-datatype datatype))]
    (reader-fn item num-elems)
    (throw (ex-info (format "Failed to find reader for datatype %s" datatype) {}))))
