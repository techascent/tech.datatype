(ns tech.datatype.reader
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting :as casting]
            [tech.datatype.io :as dtype-io]
            [tech.datatype.base :as base]
            [tech.parallel :as parallel]
            [tech.jna :as jna]
            [clojure.core.matrix.macros :refer [c-for]]
            [clojure.core.matrix.protocols :as mp])
  (:import [tech.datatype
            ObjectReader ObjectWriter Mutable
            ByteReader ByteWriter ByteMutable
            ShortReader ShortWriter ShortMutable
            IntReader IntWriter IntMutable
            LongReader LongWriter LongMutable
            FloatReader FloatWriter FloatMutable
            DoubleReader DoubleWriter DoubleMutable
            BooleanReader BooleanWriter BooleanMutable]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro reify-marshalling-numeric-reader
  [src-reader src-dtype dst-dtype dst-reader-type unchecked?]
  (let [jvm-type (casting/datatype->jvm-type dst-dtype)]
    `(if ~unchecked?
       (reify ~dst-reader-type
         (read [item# idx#]
           (casting/datatype->unchecked-jvm-cast-fn
            ~src-dtype ~dst-dtype
            (.read ~src-reader idx#)))
         (readBlock [item# offset# dest#]
           (let [dest-pos# (.position dest#)
                 n-elems# (base/ecount dest#)]
             (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                    (.put dest# (+ dest-pos# idx#)
                          (casting/datatype->unchecked-jvm-cast-fn
                           ~src-dtype ~dst-dtype
                           (.read ~src-reader (+ idx# offset#)))))
             dest#))
         (readIndexes [item# indexes# dest#]
           (let [idx-pos# (.position indexes#)
                 dest-pos# (.position dest#)
                 n-elems# (base/ecount dest#)]
             (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                    (.put dest# (+ dest-pos# idx#)
                          (casting/datatype->unchecked-jvm-cast-fn
                           ~src-dtype ~dst-dtype
                           (.read ~src-reader
                                  (+ idx#
                                     (.get indexes# (+ idx# idx-pos#)))))))
             dest#)))
       (reify ~dst-reader-type
         (read [item# idx#]
           (casting/datatype->jvm-cast-fn ~src-dtype ~dst-dtype
                                      (.read ~src-reader idx#)))
         (readBlock [item# offset# dest#]
           (let [dest-pos# (.position dest#)
                 n-elems# (base/ecount dest#)]
             (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                    (.put dest# (+ dest-pos# idx#)
                          (casting/datatype->jvm-cast-fn
                           ~src-dtype ~dst-dtype
                           (.read ~src-reader (+ idx# offset#)))))
             dest#))
         (readIndexes [item# indexes# dest#]
           (let [idx-pos# (.position indexes#)
                 dest-pos# (.position dest#)
                 n-elems# (base/ecount dest#)]
             (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                    (.put dest# (+ dest-pos# idx#)
                          (casting/datatype->jvm-cast-fn
                           ~src-dtype ~dst-dtype
                           (.read ~src-reader
                                  (+ idx#
                                     (.get indexes# (+ idx# idx-pos#)))))))
             dest#))))))


(defmacro reify-marshalling-boolean-reader
  [src-reader src-dtype unchecked?]
  `(if ~unchecked?
     (reify BooleanReader
       (read [item# idx#]
         (casting/datatype->unchecked-cast-fn ~src-dtype :boolean
                                              (.read ~src-reader idx#)))
       (readBlock [item# offset# dest#]
         (let [n-elems# (.size dest#)]
           (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                  (.set dest# idx#
                        (casting/datatype->unchecked-cast-fn
                         ~src-dtype :boolean
                         (.read ~src-reader (+ idx# offset#)))))
           dest#))
       (readIndexes [item# indexes# dest#]
         (let [n-elems# (.size dest#)
               idx-pos# (.position indexes#)]
           (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                  (.set dest# idx#
                        (casting/datatype->unchecked-cast-fn
                         ~src-dtype :boolean
                         (.read ~src-reader (.get indexes#
                                                  (+ idx-pos# idx#))))))
           dest#)))
     (reify BooleanReader
       (read [item# idx#]
         (casting/datatype->cast-fn ~src-dtype :boolean
                                    (.read ~src-reader idx#)))
       (readBlock [item# offset# dest#]
         (let [n-elems# (.size dest#)]
           (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                  (.set dest# idx#
                        (casting/datatype->cast-fn
                         ~src-dtype :boolean
                         (.read ~src-reader (+ idx# offset#)))))
           dest#))
       (readIndexes [item# indexes# dest#]
         (let [n-elems# (.size dest#)
               idx-pos# (.position indexes#)]
           (c-for [idx# (int 0) (< idx# n-elems#) (inc idx#)]
                  (.set dest# idx#
                        (casting/datatype->cast-fn
                         ~src-dtype :boolean
                         (.read ~src-reader (.get indexes#
                                                  (+ idx-pos# idx#))))))
           dest#)))))



(defmacro make-marshalling-reader
  [src-reader src-dtype dst-dtype unchecked?]
  `(let [reader# ~src-reader]
     (case ~dst-dtype
       :int8 (reify-marshalling-numeric-reader reader# ~src-dtype :int8 ByteReader ~unchecked?)
       :uint8 (reify-marshalling-numeric-reader reader# ~src-dtype :uint8 ByteReader ~unchecked?)
       :int16 (reify-marshalling-numeric-reader reader# ~src-dtype :int16 ShortReader ~unchecked?)
       :uint16 (reify-marshalling-numeric-reader reader# ~src-dtype :uint16 ShortReader ~unchecked?)
       :int32 (reify-marshalling-numeric-reader reader# ~src-dtype :int32 IntReader ~unchecked?)
       :uint32 (reify-marshalling-numeric-reader reader# ~src-dtype :uint32 IntReader ~unchecked?)
       :int64 (reify-marshalling-numeric-reader reader# ~src-dtype :int64 LongReader ~unchecked?)
       :uint64 (reify-marshalling-numeric-reader reader# ~src-dtype :uint64 LongReader ~unchecked?)
       :float32 (reify-marshalling-numeric-reader reader# ~src-dtype :float32 FloatReader ~unchecked?)
       :float64 (reify-marshalling-numeric-reader reader# ~src-dtype :float64 DoubleReader ~unchecked?)
       :boolean (reify-marshalling-boolean-reader reader# ~src-dtype ~unchecked?)
       :object (dtype-io/make-object-reader reader#))))


(defmacro make-marshalling-reader-fn
  "src-dtype is known at compile time, dst-dtype is not"
  [src-dtype]
  `(fn [src-container# dst-dtype# unchecked?#]
     (let [src-reader# (dtype-io/datatype->reader ~src-dtype src-container# true)]
       (make-marshalling-reader src-reader# ~src-dtype dst-dtype# unchecked?#))))


(defn make-reader
  [backing-store reader-datatype unchecked?]
  (if (= reader-datatype (dtype-proto/get-datatype backing-store))
    (dtype-proto/->reader-of-type backing-store reader-datatype true)
    (case (dtype-proto/get-datatype backing-store)
      :int8 ((parallel/require-resolve 'tech.datatype.readers.byte/int8-readers)
             backing-store reader-datatype unchecked?)
      :int16 ((parallel/require-resolve 'tech.datatype.readers.short/int16-readers)
              backing-store reader-datatype unchecked?)
      :int32 ((parallel/require-resolve 'tech.datatype.readers.int/int32-readers)
              backing-store reader-datatype unchecked?)
      :int64 ((parallel/require-resolve 'tech.datatype.readers.long/int64-readers)
              backing-store reader-datatype unchecked?)
      :float32 ((parallel/require-resolve 'tech.datatype.readers.float/float32-readers)
                backing-store reader-datatype unchecked?)
      :float64 ((parallel/require-resolve 'tech.datatype.readers.double/float64-readers)
                backing-store reader-datatype unchecked?)
      :boolean ((parallel/require-resolve 'tech.datatype.readers.boolean/boolean-readers)
                backing-store reader-datatype unchecked?))))
