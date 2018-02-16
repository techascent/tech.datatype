(ns tech.datatype.core
  "Generalized efficient manipulations of sequences of primitive datatype.
Includes specializations for java arrays, array views (subsection of an array)
and nio buffers.  There are specializations to allow implementations to provide
efficient full typed copy functions when the types can be ascertained.  Usually
this involves a double-dispatch on both the src and dest arguments:

  https://en.wikipedia.org/wiki/Double_dispatch.

  Generic operations include:
  1. datatype of this sequence.
  2. Writing to, reading from.
  3. Construction.
  4. Efficient mutable copy from one sequence to another."

  (:require [clojure.core.matrix.macros :refer [c-for]]
            [clojure.core.matrix.protocols :as mp]
            [clojure.core.matrix :as m]
            [tech.datatype.marshal :as marshal]
            [tech.datatype.base :as base])
  (:import [java.nio ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer Buffer]
           [mikera.arrayz INDArray]
           [tech.datatype DoubleArrayView FloatArrayView
            LongArrayView IntArrayView ShortArrayView ByteArrayView]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn get-datatype
  [item]
  (base/get-datatype item))


(def datatypes base/datatypes)


(defn make-array-of-type
  [datatype elem-count-or-seq]
  (base/make-array-of-type datatype elem-count-or-seq))


(defn ecount
  [item]
  (base/ecount item))


(defn ->view
  [& args]
  (apply base/->view args))


(defn make-view
  [datatype item-count-or-seq]
  (base/make-view datatype item-count-or-seq))


(defn copy!
  [& args]
  (apply base/copy! args))


(defn ecount
  ^long [val]
  (base/ecount val))


(defn copy!
  [& args]
  (apply base/copy! args))


(defn copy-raw->item!
  [raw-data ary-target target-offset]
  (base/copy-raw->item! raw-data ary-target target-offset))


(defn ->view
  [& args]
  (apply base/->view args))


(defn set-value! [item offset value]
  (base/set-value! item offset value))


(defn set-constant! [item offset value elem-count]
  (base/set-constant! item offset value elem-count))


(defn get-value [item offset]
  (base/get-value item offset))



(extend-protocol base/PDatatype
  ByteBuffer
  (get-datatype [item] :byte)
  ShortBuffer
  (get-datatype [item] :short)
  IntBuffer
  (get-datatype [item] :int)
  LongBuffer
  (get-datatype [item] :long)
  FloatBuffer
  (get-datatype [item] :float)
  LongBuffer
  (get-datatype [item] :long)
  Byte
  (get-datatype [item] :byte)
  Short
  (get-datatype [item] :short)
  Integer
  (get-datatype [item] :int)
  Long
  (get-datatype [item] :long)
  Float
  (get-datatype [item] :float)
  Double
  (get-datatype [item] :double))


(def datatype->primitive-type-map
  {:byte Byte/TYPE
   :short Short/TYPE
   :int Integer/TYPE
   :long Long/TYPE
   :float Float/TYPE
   :double Double/TYPE})

(defn datatype->primitive-type
  [datatype]
  (get datatype->primitive-type-map datatype))


(defmacro set-array-constant-impl
  [ary offset val cast-fn elem-count]
  `(let [ary# ~ary
         offset# (long ~offset)
         val# (~cast-fn ~val)
         elem-count# (long ~elem-count)]
     (c-for
      [idx# 0 (< idx# elem-count#) (inc idx#)]
      (aset ary# (+ offset# idx#) val#))))


(extend-type ByteArrayView
  base/PDatatype
  (get-datatype [item] :byte)
  base/PAccess
  (set-value! [item in-offset value] (aset (.data item) (+ (long in-offset)
                                                           (.offset item)) (byte value)))
  (set-constant! [item in-offset value elem-count]
    (set-array-constant-impl (.data item) (+ (long in-offset) (.offset item))
                             value byte elem-count))
  (get-value [item in-offset] (aget (.data item) (+ (.offset item) (long in-offset))))
  mp/PElementCount
  (element-count [item] (.length item))
  base/PView
  (->view-impl [item n-offset length] (ByteArrayView. (.data item)
                                                      (+ (.offset item) (long n-offset))
                                                      length)))

(extend-type ShortArrayView
  base/PDatatype
  (get-datatype [item] :short)
  base/PAccess
  (set-value! [item in-offset value] (aset (.data item) (+ (long in-offset) (.offset item))
                                           (short value)))
  (set-constant! [item in-offset value elem-count]
    (set-array-constant-impl (.data item) (+ (long in-offset) (.offset item))
                             value short elem-count))
  (get-value [item in-offset] (aget (.data item) (+ (.offset item) (long in-offset))))
  mp/PElementCount
  (element-count [item] (.length item))
  base/PView
  (->view-impl [item n-offset length] (ShortArrayView. (.data item) (+ (.offset item)
                                                                       (long n-offset))
                                                       length)))

(extend-type IntArrayView
  base/PDatatype
  (get-datatype [item] :int)
  base/PAccess
  (set-value! [item in-offset value] (aset (.data item) (+ (long in-offset) (.offset item))
                                           (int value)))
  (set-constant! [item in-offset value elem-count]
    (set-array-constant-impl (.data item) (+ (long in-offset) (.offset item))
                             value int elem-count))
  (get-value [item in-offset] (aget (.data item) (+ (.offset item) (long in-offset))))
  mp/PElementCount
  (element-count [item] (.length item))
  base/PView
  (->view-impl [item n-offset length] (IntArrayView. (.data item) (+ (.offset item)
                                                                     (long n-offset)) length)))

(extend-type LongArrayView
  base/PDatatype
  (get-datatype [item] :long)
  base/PAccess
  (set-value! [item in-offset value] (aset (.data item) (+ (long in-offset) (.offset item))
                                           (long value)))
  (set-constant! [item in-offset value elem-count]
    (set-array-constant-impl (.data item) (+ (long in-offset) (.offset item)) value
                             long elem-count))
  (get-value [item in-offset] (aget (.data item) (+ (.offset item) (long in-offset))))
  mp/PElementCount
  (element-count [item] (.length item))
  base/PView
  (->view-impl [item n-offset length] (LongArrayView. (.data item) (+ (.offset item)
                                                                      (long n-offset)) length)))

(extend-type FloatArrayView
  base/PDatatype
  (get-datatype [item] :float)
  base/PAccess
  (set-value! [item in-offset value] (aset (.data item) (+ (long in-offset) (.offset item))
                                           (float value)))
  (set-constant! [item in-offset value elem-count]
    (set-array-constant-impl (.data item) (+ (long in-offset) (.offset item)) value
                             float elem-count))
  (get-value [item in-offset] (aget (.data item) (+ (.offset item) (long in-offset))))
  mp/PElementCount
  (element-count [item] (.length item))
  base/PView
  (->view-impl [item n-offset length] (FloatArrayView. (.data item) (+ (.offset item)
                                                                       (long n-offset)) length)))

(extend-type DoubleArrayView
  base/PDatatype
  (get-datatype [item] :double)
  base/PAccess
  (set-value! [item in-offset value] (aset (.data item) (+ (long in-offset) (.offset item))
                                           (double value)))
  (set-constant! [item in-offset value elem-count]
    (set-array-constant-impl (.data item) (+ (long in-offset) (.offset item)) value
                             double elem-count))
  (get-value [item in-offset] (aget (.data item) (+ (.offset item) (long in-offset))))
  mp/PElementCount
  (element-count [item] (.length item))
  base/PView
  (->view-impl [item n-offset n-length] (DoubleArrayView. (.data item) (+ (.offset item)
                                                                          (long n-offset))
                                                          n-length)))


;;Macros to use to use the sub views as efficiently as one uses arrays.
(defmacro v-aset
  [array-view item-offset value]
  `(.set ~array-view ~item-offset ~value))


(defmacro v-aget
  [array-view item-offset]
  `(.get ~array-view ~item-offset))


(defmacro v-alength
  [array-view]
  `(.length ~array-view))


(extend-type (Class/forName "[B")
  base/PDatatype
  (get-datatype [item] :byte)
  base/PAccess
  (set-value! [item ^long offset value] (aset ^bytes item offset (byte value)))
  (set-constant! [item ^long offset value ^long elem-count]
    (let [^bytes item item]
      (set-array-constant-impl item offset value byte elem-count)))
  (get-value [item ^long offset] (aget ^bytes item offset))
  base/PView
  (->view-impl [item offset length] (ByteArrayView. item offset length))
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset]
    (base/raw-dtype-copy! raw-data ary-target target-offset)))

(extend-type (Class/forName "[S")
  base/PDatatype
  (get-datatype [item] :short)
  base/PAccess
  (set-value! [item ^long offset value] (aset ^shorts item offset (short value)))
  (set-constant! [item ^long offset value ^long elem-count]
    (let [^shorts item item]
      (set-array-constant-impl item offset value short elem-count)))
  (get-value [item ^long offset] (aget ^shorts item offset))
  base/PView
  (->view-impl [item offset length] (ShortArrayView. item offset length))
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset]
    (base/raw-dtype-copy! raw-data ary-target target-offset)))

(extend-type (Class/forName "[I")
  base/PDatatype
  (get-datatype [item] :int)
  base/PAccess
  (set-value! [item ^long offset value] (aset ^ints item offset (int value)))
  (set-constant! [item ^long offset value ^long elem-count]
    (let [^ints item item]
      (set-array-constant-impl item offset value int elem-count)))
  (get-value [item ^long offset] (aget ^ints item offset))
  base/PView
  (->view-impl [item offset length] (IntArrayView. item offset length))
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset]
    (base/raw-dtype-copy! raw-data ary-target target-offset)))

(extend-type (Class/forName "[J")
  base/PDatatype
  (get-datatype [item] :long)
  base/PAccess
  (set-value! [item ^long offset value] (aset ^longs item offset (long value)))
  (set-constant! [item ^long offset value ^long elem-count]
    (let [^longs item item]
      (set-array-constant-impl item offset value long elem-count)))
  (get-value [item ^long offset] (aget ^longs item offset))
  base/PView
  (->view-impl [item offset length] (LongArrayView. item offset length))
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset]
    (base/raw-dtype-copy! raw-data ary-target target-offset)))

(extend-type (Class/forName "[F")
  base/PDatatype
  (get-datatype [item] :float)
  base/PAccess
  (set-value! [item ^long offset value] (aset ^floats item offset (float value)))
  (set-constant! [item ^long offset value ^long elem-count]
    (let [^floats item item]
      (set-array-constant-impl item offset value float elem-count)))
  (get-value [item ^long offset] (aget ^floats item offset))
  base/PView
  (->view-impl [item offset length] (FloatArrayView. item offset length))
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset]
    (base/raw-dtype-copy! raw-data ary-target target-offset)))

(extend-type (Class/forName "[D")
  base/PDatatype
  (get-datatype [item] :double)
  base/PAccess
  (set-value! [item ^long offset value] (aset ^doubles item offset (double value)))
  (set-constant! [item ^long offset value ^long elem-count]
    (let [^doubles item item]
      (set-array-constant-impl item offset value double elem-count)))
  (get-value [item ^long offset] (aget ^doubles item offset))
  base/PView
  (->view-impl [item offset length] (DoubleArrayView. item offset length))
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset]
    (base/raw-dtype-copy! raw-data ary-target target-offset)))


(defn datatype->cast-fn
  [datatype]
  (cond
    (= datatype :byte) byte
    (= datatype :short) short
    (= datatype :int) int
    (= datatype :long) long
    (= datatype :float) float
    (= datatype :double) double))

(defn cast-to
  "cast to. boxes object if datatype is runtime variable"
  [value datatype]
  ((datatype->cast-fn datatype) value))

(defmacro cast-to-m
  "cast to, potentially keep unboxed if datatype is known at compile time"
  [value datatype]
  (cond
    (= datatype :byte) `(byte ~value)
    (= datatype :short) `(short ~value)
    (= datatype :int) `(int ~value)
    (= datatype :long) `(long ~value)
    (= datatype :float) `(float ~value)
    (= datatype :double) `(double ~value)))


(defmacro set-buffer-constant-impl
  [buffer offset value cast-fn elem-count]
  `(let [~value (~cast-fn ~value)]
     (loop [idx# 0]
       (when (< idx# ~elem-count)
         (.put ~buffer (+ ~offset idx#) ~value)
         (recur (inc idx#))))))


(extend-type Buffer
  mp/PElementCount
  (element-count [item] (.remaining item))
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset]
    (base/raw-dtype-copy! raw-data ary-target target-offset)))


(extend-type ByteBuffer
  base/PDatatype
  (get-datatype [item] :byte)
  base/PAccess
  (set-value! [item ^long offset value] (.put ^ByteBuffer item offset (byte value)))
  (set-constant! [item ^long offset value ^long elem-count]
    (let [^ByteBuffer item item]
      (set-buffer-constant-impl item offset value byte elem-count)))
  (get-value [item ^long offset] (.get ^ByteBuffer item offset))
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset]
    (base/raw-dtype-copy! raw-data ary-target target-offset)))


(extend-type ShortBuffer
  base/PDatatype
  (get-datatype [item] :short)
  base/PAccess
  (set-value! [item ^long offset value] (.put ^ShortBuffer item offset (short value)))
  (set-constant! [item ^long offset value ^long elem-count]
    (let [^ShortBuffer item item]
      (set-buffer-constant-impl item offset value short elem-count)))
  (get-value [item ^long offset] (.get ^ShortBuffer item offset))
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset]
    (base/raw-dtype-copy! raw-data ary-target target-offset)))


(extend-type IntBuffer
  base/PDatatype
  (get-datatype [item] :int)
  base/PAccess
  (set-value! [item ^long offset value] (.put ^IntBuffer item offset (int value)))
  (set-constant! [item ^long offset value ^long elem-count]
    (let [^IntBuffer item item]
      (set-buffer-constant-impl item offset value int elem-count)))
  (get-value [item ^long offset] (.get ^IntBuffer item offset))
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset]
    (base/raw-dtype-copy! raw-data ary-target target-offset)))


(extend-type LongBuffer
  base/PDatatype
  (get-datatype [item] :long)
  base/PAccess
  (set-value! [item ^long offset value] (.put ^LongBuffer item offset (long value)))
  (set-constant! [item ^long offset value ^long elem-count]
    (let [^LongBuffer item item]
      (set-buffer-constant-impl item offset value long elem-count)))
  (get-value [item ^long offset] (.get ^LongBuffer item offset))
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset]
    (base/raw-dtype-copy! raw-data ary-target target-offset)))


(extend-type FloatBuffer
  base/PDatatype
  (get-datatype [item] :float)
  base/PAccess
  (set-value! [item ^long offset value] (.put ^FloatBuffer item offset (float value)))
  (set-constant! [item ^long offset value ^long elem-count]
    (let [^FloatBuffer item item]
      (set-buffer-constant-impl item offset value float elem-count)))
  (get-value [item ^long offset] (.get ^FloatBuffer item offset))
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset]
    (base/raw-dtype-copy! raw-data ary-target target-offset)))


(extend-type DoubleBuffer
  base/PDatatype
  (get-datatype [item] :double)
  base/PAccess
  (set-value! [item ^long offset value] (.put ^DoubleBuffer item offset (double value)))
  (set-constant! [item ^long offset value ^long elem-count]
    (let [^DoubleBuffer item item]
      (set-buffer-constant-impl item offset value double elem-count)))
  (get-value [item ^long offset] (.get ^DoubleBuffer item offset))
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset]
    (base/raw-dtype-copy! raw-data ary-target target-offset)))


(extend-protocol base/PCopyRawData
  Buffer
  (copy-raw->item! [raw-data ary-target target-offset]
    (base/raw-dtype-copy! raw-data ary-target target-offset))
  INDArray
  (copy-raw->item! [raw-data ary-target target-offset]
    (let [^doubles item-data (or (mp/as-double-array raw-data)
                                 (mp/to-double-array raw-data))]
      (copy-raw->item! item-data ary-target target-offset))))


(extend-type Object
  base/PCopyRawData
  (copy-raw->item!
   [src-data dst-data offset]
   (base/copy-raw->item! (seq src-data) dst-data offset)))
