(ns tech.datatype.java-primitive
  "Java specific mappings of the datatype system."
  (:require [tech.datatype.base-macros :as base-macros]
            [tech.datatype.base :as base]
            [clojure.core.matrix.macros :refer [c-for]]
            [clojure.core.matrix.protocols :as mp])
  (:import [java.nio ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer Buffer]
           [mikera.arrayz INDArray]))


(set! *warn-on-reflection* true)


(defprotocol PToBuffer
  "Take a 'thing' and convert it to a nio buffer."
  (->buffer [item]))



(defn raw-dtype-copy!
  [raw-data ary-target ^long target-offset options]
  (base/copy! raw-data 0 ary-target target-offset (base/ecount raw-data) options)
  [ary-target (+ target-offset ^long (base/ecount raw-data))])



(extend-type Object
  base/PContainerType
  (container-type [item] :object)
  base/PCopyRawData
  (copy-raw->item!
   [src-data dst-data offset options]
    (base/copy-raw->item! (seq src-data) dst-data offset options)))


(extend-type Buffer
  base/PContainerType
  (container-type [item] :nio-buffer)
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (raw-dtype-copy! raw-data ary-target target-offset options))
  mp/PElementCount
  (element-count [item] (.remaining item)))


(extend-type INDArray
  base/PDatatype
  (get-datatype [item] :float64)
  base/PContainerType
  (container-type [item] :mikeral-n-dimensional-array)
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (let [^doubles item-data (or (mp/as-double-array raw-data)
                                 (mp/to-double-array raw-data))]
      (raw-dtype-copy! item-data ary-target target-offset options))))


(def java-primitive-datatypes
  [{:name :int8
    :byte-size 1
    :numeric-type :integer}
   {:name :int16
    :byte-size 2
    :numeric-type :integer}
   {:name :int32
    :byte-size 4
    :numeric-type :integer}
   {:name :int64
    :byte-size 8
    :numeric-type :integer}
   {:name :float32
    :byte-size 4
    :numeric-type :float}
   {:name :float64
    :byte-size 8
    :numeric-type :float}])


(doseq [{:keys [name byte-size]} java-primitive-datatypes]
  (base/add-datatype->size-mapping name byte-size))


(defn make-array-of-type
  [datatype elem-count-or-seq]
  (case datatype
    :int8 (byte-array elem-count-or-seq)
    :int16 (short-array elem-count-or-seq)
    :int32 (int-array elem-count-or-seq)
    :int64 (long-array elem-count-or-seq)
    :float32 (float-array elem-count-or-seq)
    :float64 (double-array elem-count-or-seq)))


(defn make-buffer-of-type
  [datatype elem-count-or-seq]
  (->buffer (make-array-of-type datatype elem-count-or-seq)))


;;Provide type hinted access to container

(defn as-byte-buffer
  ^ByteBuffer [obj] obj)

(defn as-short-buffer
  ^ShortBuffer [obj] obj)

(defn as-int-buffer
  ^IntBuffer [obj] obj)

(defn as-long-buffer
  ^LongBuffer [obj] obj)

(defn as-float-buffer
  ^FloatBuffer [obj] obj)

(defn as-double-buffer
  ^DoubleBuffer [obj] obj)

(defn as-byte-array
  ^bytes [obj] obj)

(defn as-short-array
  ^shorts [obj] obj)

(defn as-int-array
  ^ints [obj] obj)

(defn as-long-array
  ^longs [obj] obj)

(defn as-float-array
  ^floats [obj] obj)

(defn as-double-array
  ^doubles [obj] obj)


(defmacro datatype->array-cast-fn
  [dtype buf]
  (condp = dtype
    :int8 `(as-byte-array ~buf)
    :int16 `(as-short-array ~buf)
    :int32 `(as-int-array ~buf)
    :int64 `(as-long-array ~buf)
    :float32 `(as-float-array ~buf)
    :float64 `(as-double-array ~buf)))



(defmacro datatype->buffer-cast-fn
  [dtype buf]
  (condp = dtype
    :int8 `(as-byte-buffer ~buf)
    :int16 `(as-short-buffer ~buf)
    :int32 `(as-int-buffer ~buf)
    :int64 `(as-long-buffer ~buf)
    :float32 `(as-float-buffer ~buf)
    :float64 `(as-double-buffer ~buf)))


;; Save these because we are switching to unchecked soon.
(def int8-cast byte)
(def int16-cast short)
(def int32-cast int)
(def int64-cast long)
(def float32-cast float)
(def float64-cast double)


(base/add-cast-fn :int8 byte)
(base/add-cast-fn :int16 short)
(base/add-cast-fn :int32 int)
(base/add-cast-fn :int64 long)
(base/add-cast-fn :float32 float)
(base/add-cast-fn :float64 double)


;; From this point on everything else is unchecked-checked!!
;; This is what allows this one file to provide both checked and unchecked operations.
;; The reason the rest of the file is unchecked is to provide faster iteration across
;; array access.
(set! *unchecked-math* :warn-on-boxed)


(base/add-unchecked-cast-fn :int8 unchecked-byte)
(base/add-unchecked-cast-fn :int16 unchecked-short)
(base/add-unchecked-cast-fn :int32 unchecked-int)
(base/add-unchecked-cast-fn :int64 unchecked-long)
(base/add-unchecked-cast-fn :float32 unchecked-float)
(base/add-unchecked-cast-fn :float64 unchecked-double)


(defmacro datatype->cast-fn
  [src-dtype dtype val]
  (if (= src-dtype dtype)
    val
    (case dtype
      :int8 `(unchecked-byte (int8-cast ~val))
      :int16 `(unchecked-short (int16-cast ~val))
      :int32 `(unchecked-int (int32-cast ~val))
      :int64 `(unchecked-long (int64-cast ~val))
      :float32 `(unchecked-float (float32-cast ~val))
      :float64 `(unchecked-double (float64-cast ~val)))))


(defmacro datatype->unchecked-cast-fn
  [src-dtype dtype val]
  (if (= src-dtype dtype)
    val
    (case dtype
      :int8 `(unchecked-byte ~val)
      :int16 `(unchecked-short ~val)
      :int32 `(unchecked-int ~val)
      :int64 `(unchecked-long ~val)
      :float32 `(unchecked-float ~val)
      :float64 `(unchecked-double ~val))))


(defmacro datatype->buffer-creation
  [datatype src-ary]
  (case datatype
    :int8 `(ByteBuffer/wrap ^bytes ~src-ary)
    :int16 `(ShortBuffer/wrap ^shorts ~src-ary)
    :int32 `(IntBuffer/wrap ^ints ~src-ary)
    :int64 `(LongBuffer/wrap ^longs ~src-ary)
    :float32 `(FloatBuffer/wrap ^floats ~src-ary)
    :float64 `(DoubleBuffer/wrap ^doubles ~src-ary)))


(defmacro implement-array-type
  [array-class datatype]
  `(clojure.core/extend
       ~array-class
     base/PDatatype
     {:get-datatype (fn [arg#] ~datatype)}
     base/PContainerType
     {:container-type (fn [item#] :java-array)}
     base/PAccess
     {:get-value (fn [item# ^long idx#]
                   (aget (datatype->array-cast-fn ~datatype item#) idx#))
      :set-value! (fn [item# ^long offset# value#]
                    (aset (datatype->array-cast-fn ~datatype item#) offset#
                          (datatype->cast-fn :ignored ~datatype value#)))
      :set-constant! (fn [item# ^long offset# value# ^long elem-count#]
                       (let [value# (datatype->cast-fn :ignored ~datatype value#)
                             item# (datatype->array-cast-fn ~datatype item#)
                             offset# (long offset#)
                             elem-count# (+ (long elem-count#)
                                            offset#)]
                         (c-for [idx# offset# (< idx# elem-count#) (+ idx# 1)]
                                (aset item# idx# value#))))}
     base/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (let [copy-len# (alength (datatype->array-cast-fn ~datatype raw-data#))]
                           (base/copy! raw-data# 0 ary-target# target-offset#
                                       copy-len# options#)
                           [ary-target# (+ (long target-offset#) copy-len#)]))}
     PToBuffer
     {:->buffer (fn [src-ary#]
                  (datatype->buffer-creation ~datatype src-ary#))}))


(implement-array-type (Class/forName "[B") :int8)
(implement-array-type (Class/forName "[S") :int16)
(implement-array-type (Class/forName "[I") :int32)
(implement-array-type (Class/forName "[J") :int64)
(implement-array-type (Class/forName "[F") :float32)
(implement-array-type (Class/forName "[D") :float64)


(defmacro implement-buffer-type
  [buffer-class datatype]
  `(clojure.core/extend
       ~buffer-class
     base/PDatatype
     {:get-datatype (fn [arg#] ~datatype)}
     base/PAccess
     {:get-value (fn [item# ^long idx#]
                   (.get (datatype->buffer-cast-fn ~datatype item#) idx#))
      :set-value! (fn [item# ^long offset# value#]
                    (.put (datatype->buffer-cast-fn ~datatype item#) offset#
                          (datatype->cast-fn :ignored ~datatype value#)))
      :set-constant! (fn [item# ^long offset# value# ^long elem-count#]
                       (let [value# (datatype->cast-fn :ignored ~datatype value#)
                             item# (datatype->buffer-cast-fn ~datatype item#)
                             offset# (long offset#)
                             elem-count# (+ (long elem-count#)
                                            offset#)]
                         (c-for [idx# offset# (< idx# elem-count#) (+ idx# 1)]
                                (.put item# idx# value#))))}
     base/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (let [copy-len# (.remaining (datatype->buffer-cast-fn ~datatype raw-data#))]
                           (base/copy! raw-data# 0 ary-target# target-offset#
                                       copy-len# options#)
                           [ary-target# (+ (long target-offset#) copy-len#)]))}
     PToBuffer
     {:->buffer (fn [item#] item#)}))


(implement-buffer-type ByteBuffer :int8)
(implement-buffer-type ShortBuffer :int16)
(implement-buffer-type IntBuffer :int32)
(implement-buffer-type LongBuffer :int64)
(implement-buffer-type FloatBuffer :float32)
(implement-buffer-type DoubleBuffer :float64)


;;Implement dtype-x-dtype copy operation table

(def datatypes (mapv :name java-primitive-datatypes))


(base/add-container-conversion-fn
 :java-array :nio-buffer
 (fn [dst-type src-ary]
   [(->buffer src-ary) 0]))


(defmacro buffer-buffer-copy
  [src-dtype dst-dtype unchecked? src src-offset dst dst-offset n-elems options]
  (if unchecked?
    `(let [src# (datatype->buffer-cast-fn ~src-dtype ~src)
           dst# (datatype->buffer-cast-fn ~dst-dtype ~dst)
           src-offset# (long ~src-offset)
           dst-offset# (long ~dst-offset)
           n-elems# (long ~n-elems)]
       (c-for [idx# 0 (< idx# n-elems#) (unchecked-add idx# 1)]
              (.put dst# (unchecked-add idx# dst-offset#)
                    (datatype->unchecked-cast-fn ~src-dtype ~dst-dtype
                                                 (.get src# (unchecked-add idx# src-offset#))))))
    `(let [src# (datatype->buffer-cast-fn ~src-dtype ~src)
           dst# (datatype->buffer-cast-fn ~dst-dtype ~dst)
           src-offset# (long ~src-offset)
           dst-offset# (long ~dst-offset)
           n-elems# (long ~n-elems)]
       (c-for [idx# 0 (< idx# n-elems#) (unchecked-add idx# 1)]
              (.put dst# (unchecked-add idx# dst-offset#)
                    (datatype->cast-fn ~src-dtype ~dst-dtype
                                                 (.get src# (unchecked-add idx# src-offset#))))))))


(defmacro update-base-copy-table
  []
  `(vector
    ~@(for [src-dtype datatypes
            dst-dtype datatypes
            unchecked? [false true]]
        `(do (base/add-copy-operation :nio-buffer :nio-buffer ~src-dtype ~dst-dtype ~unchecked?
                                      (fn [src# src-offset# dst# dst-offset# n-elems# options#]
                                        (buffer-buffer-copy ~src-dtype ~dst-dtype ~unchecked?
                                                            src# src-offset# dst# dst-offset# n-elems# options#)))
             [:nio-buffer :nio-buffer ~src-dtype ~dst-dtype ~unchecked?]))))


(def core-copy-operations (update-base-copy-table))
