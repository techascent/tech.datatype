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
            [tech.datatype.io :as dtype-io]
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
     (reify ~writer-type
       (getDatatype [writer#] ~intermediate-datatype)
       (size [writer#] (int (mp/element-count ~buffer)))
       (write [writer# idx# value#]
         (cls-type->write-fn ~buffer-type ~buffer idx# ~buffer-pos
          (unchecked-full-cast value# ~writer-datatype
                               ~intermediate-datatype
                               ~buffer-datatype))))
     (reify ~writer-type
       (getDatatype [writer#] ~intermediate-datatype)
       (size [writer#] (int (mp/element-count ~buffer)))
       (write [writer# idx# value#]
         (cls-type->write-fn ~buffer-type ~buffer idx# ~buffer-pos
                             (checked-full-write-cast value# ~writer-datatype
                                                      ~intermediate-datatype
                                                      ~buffer-datatype))))))


(defmacro make-buffer-writer-table
  []
  `(->> [~@(for [dtype (casting/all-datatypes)
                 buffer-datatype casting/host-numeric-types]
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
                   unchecked?#)))])]
        (into {})))



(def buffer-writer-table (make-buffer-writer-table))


(defmacro make-list-writer-table
  []
  `(->> [~@(for [dtype (casting/all-datatypes)
                 buffer-datatype casting/all-host-datatypes]
            [[buffer-datatype dtype]
             `(fn [buffer# unchecked?#]
                (let [buffer# (typecast/datatype->list-cast-fn ~buffer-datatype buffer#)]
                  (make-buffer-writer
                   ~(typecast/datatype->writer-type dtype)
                   ~(typecast/datatype->list-type buffer-datatype)
                   buffer# buffer-pos#
                   ~(casting/datatype->safe-host-type dtype) ~dtype
                   ~buffer-datatype
                   unchecked?#)))])]
        (into {})))


(def list-writer-table (make-list-writer-table))



(defmacro make-marshalling-writer
  [dst-writer result-dtype intermediate-dtype src-dtype src-writer-type
   unchecked?]
  `(if ~unchecked?
     (reify ~src-writer-type
       (getDatatype [item#] ~intermediate-dtype)
       (size [item#] (.size ~dst-writer))
       (write[item# idx# value#]
         (.write ~dst-writer idx#
                 (unchecked-full-cast value# ~src-dtype ~intermediate-dtype ~result-dtype))))
     (reify ~src-writer-type
       (getDatatype [item#] ~intermediate-dtype)
       (size [item#] (.size ~dst-writer))
       (write[item# idx# value#]
         (.write ~dst-writer idx#
                 (checked-full-write-cast value# ~src-dtype ~intermediate-dtype ~result-dtype))))))


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



(defmacro extend-writer-type
  [writer-type datatype]
  `(clojure.core/extend
       ~writer-type
     dtype-proto/PToWriter
     {:->writer-of-type
      (fn [item# dtype# unchecked?#]
        (if (= dtype# (dtype-proto/get-datatype item#))
          item#
          (if-let [writer-fn# (get marshalling-writer-table [~datatype dtype#])]
            (writer-fn# item# unchecked?#)
            (throw (ex-info (format "Failed to find marshalling writer: %s %s" ~datatype dtype#)
                            {:src-datatype ~datatype
                             :dst-datatype dtype#})))))}))


(extend-writer-type ByteWriter :int8)
(extend-writer-type ShortWriter :int16)
(extend-writer-type IntWriter :int32)
(extend-writer-type LongWriter :int64)
(extend-writer-type FloatWriter :float32)
(extend-writer-type DoubleWriter :float64)
(extend-writer-type BooleanWriter :boolean)
(extend-writer-type ObjectWriter :object)
