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
  "Take a 'thing' and convert it to a nio buffer.  Only valid if the thing
  shares the backing store with the buffer.  Result may not exactly
  represent the value of the item itself as the backing store may require
  element-by-element conversion to represent the value of the item."
  (->buffer-backing-store [item]))


(defprotocol PToArray
  "Take a'thing' and convert it to an array that exactly represents the value
  of the data."
  (->array [item]
    "Convert to an array; both objects must share backing store")
  (->array-copy [item]
    "Convert to an array containing a copy of the data"))


(defn raw-dtype-copy!
  [raw-data ary-target ^long target-offset options]
  (base/copy! raw-data 0 ary-target target-offset (base/ecount raw-data) options)
  [ary-target (+ target-offset ^long (base/ecount raw-data))])



(extend-type Object
  base/PCopyRawData
  (copy-raw->item!
   [src-data dst-data offset options]
    (base/copy-raw->item! (seq src-data) dst-data offset options))
  base/PPersistentVector
  (->vector [src] (vec (or (->array src)
                           (->array-copy src))))
  PToBuffer
  (->buffer-backing-store [src]
    (when-let [ary-data (->array src)]
      (->buffer-backing-store src))))


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
  (container-type [item] :mikera-n-dimensional-array)
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (let [^doubles item-data (->array raw-data)]
      (raw-dtype-copy! item-data ary-target target-offset options)))
  PToArray
  (->array [item]
    (mp/as-double-array item))
  (->array-copy [item]
    (mp/to-double-array item))
  PToBuffer
  (->buffer-backing-store [item]
    (if-let [ary-data (->array item)]
      (->buffer-backing-store ary-data)
      (throw (ex-info "Item cannot be represented as a buffer"
                      {})))))


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


(def datatypes (set (map :name java-primitive-datatypes)))


(defn is-jvm-datatype?
  [dtype]
  (boolean (datatypes dtype)))


(doseq [{:keys [name byte-size]} java-primitive-datatypes]
  (base/add-datatype->size-mapping name byte-size))


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


(defn make-array-of-type
  ([datatype elem-count-or-seq options]
   (let [elem-count-or-seq (if (or (number? elem-count-or-seq)
                                   (:unchecked? options))
                             elem-count-or-seq
                             (map #(base/cast % datatype) elem-count-or-seq))]
     (case datatype
       :int8 (byte-array elem-count-or-seq)
       :int16 (short-array elem-count-or-seq)
       :int32 (int-array elem-count-or-seq)
       :int64 (long-array elem-count-or-seq)
       :float32 (float-array elem-count-or-seq)
       :float64 (double-array elem-count-or-seq))))
  ([datatype elem-count-or-seq]
   (make-array-of-type datatype elem-count-or-seq {})))


(defn make-buffer-of-type
  ([datatype elem-count-or-seq options]
   (->buffer-backing-store
    (make-array-of-type datatype elem-count-or-seq options)))
  ([datatype elem-count-or-seq]
   (make-buffer-of-type datatype elem-count-or-seq {})))


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
                         (let [copy-len# (alength (datatype->array-cast-fn ~datatype
                                                                           raw-data#))]
                           (base/copy! raw-data# 0 ary-target# target-offset#
                                       copy-len# options#)
                           [ary-target# (+ (long target-offset#) copy-len#)]))}
     base/PPersistentVector
     {:->vector (fn [src-ary#] (vec src-ary#))}
     PToBuffer
     {:->buffer-backing-store (fn [src-ary#]
                  (datatype->buffer-creation ~datatype src-ary#))}
     PToArray
     {:->array identity
      :->array-copy (fn [src-ary#]
                      (let [dst-ary# (make-array-of-type ~datatype
                                                         (mp/element-count src-ary#))]
                        (base/copy! src-ary# dst-ary#)))}))


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
                   (let [buf# (datatype->buffer-cast-fn ~datatype item#)]
                     (.get buf# (+ idx# (.position buf#)))))
      :set-value! (fn [item# ^long offset# value#]
                    (let [buf# (datatype->buffer-cast-fn ~datatype item#)]
                      (.put buf# (+ (.position buf#) offset#)
                            (datatype->cast-fn :ignored ~datatype value#))))
      :set-constant! (fn [item# ^long offset# value# ^long elem-count#]
                       (let [value# (datatype->cast-fn :ignored ~datatype value#)
                             item# (datatype->buffer-cast-fn ~datatype item#)
                             offset# (long offset#)
                             elem-count# (+ (long elem-count#)
                                            offset#)]
                         (c-for [idx# offset# (< idx# elem-count#) (+ idx# 1)]
                                (.put item# (+ idx# (.position item#)) value#))))}
     base/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (let [copy-len# (.remaining (datatype->buffer-cast-fn
                                                      ~datatype raw-data#))]
                           (base/copy! raw-data# 0 ary-target# target-offset#
                                       copy-len# options#)
                           [ary-target# (+ (long target-offset#) copy-len#)]))}
     PToBuffer
     {:->buffer-backing-store (fn [item#] item#)}
     PToArray
     {:->array (fn [item#]
                 (let [item# (datatype->buffer-cast-fn ~datatype item#)]
                   (when (and (= 0 (.position item#))
                              (not (.isDirect item#)))
                     (.array item#))))
      :->array-copy (fn [item#]
                      (let [dst-ary# (make-array-of-type ~datatype
                                                         (mp/element-count item#))]
                        (base/copy! item# dst-ary#)))}))


(implement-buffer-type ByteBuffer :int8)
(implement-buffer-type ShortBuffer :int16)
(implement-buffer-type IntBuffer :int32)
(implement-buffer-type LongBuffer :int64)
(implement-buffer-type FloatBuffer :float32)
(implement-buffer-type DoubleBuffer :float64)


;;Implement dtype-x-dtype copy operation table


(base/add-container-conversion-fn
 :java-array :nio-buffer
 (fn [dst-type src-ary]
   [(->buffer-backing-store src-ary) 0]))


(defmacro buffer-buffer-copy
  [src-dtype dst-dtype unchecked? src src-offset dst dst-offset n-elems options]
  (if unchecked?
    `(let [
           n-elems# (long ~n-elems)
           src-buf# (datatype->buffer-cast-fn ~src-dtype ~src)
           dst-buf# (datatype->buffer-cast-fn ~dst-dtype ~dst)
           src# (datatype->array-cast-fn ~src-dtype (->array ~src))
           dst# (datatype->array-cast-fn ~dst-dtype (->array ~dst))
           src-offset# (+ (.position src-buf#) (long ~src-offset))
           dst-offset# (+ (.position dst-buf#) (long ~dst-offset))]
       ;;Fast path if both are representable by java arrays
       (if (and src# dst#)
         (c-for [idx# 0 (< idx# n-elems#) (unchecked-add idx# 1)]
                (aset dst# (unchecked-add idx# dst-offset#)
                      (datatype->unchecked-cast-fn ~src-dtype ~dst-dtype
                                                   (aget src# (unchecked-add
                                                               idx# src-offset#)))))
         (let [src# (datatype->buffer-cast-fn ~src-dtype ~src)
               dst# (datatype->buffer-cast-fn ~dst-dtype ~dst)]
           (c-for [idx# 0 (< idx# n-elems#) (unchecked-add idx# 1)]
                  (.put dst# (unchecked-add idx# dst-offset#)
                        (datatype->unchecked-cast-fn ~src-dtype ~dst-dtype
                                                     (.get src# (unchecked-add
                                                                 idx# src-offset#))))))))
    `(let [src# (datatype->buffer-cast-fn ~src-dtype ~src)
           dst# (datatype->buffer-cast-fn ~dst-dtype ~dst)
           src-buf# (datatype->buffer-cast-fn ~src-dtype ~src)
           dst-buf# (datatype->buffer-cast-fn ~dst-dtype ~dst)
           src-offset# (+ (.position src-buf#) (long ~src-offset))
           dst-offset# (+ (.position dst-buf#) (long ~dst-offset))
           n-elems# (long ~n-elems)]
       (c-for [idx# 0 (< idx# n-elems#) (unchecked-add idx# 1)]
              (.put dst# (unchecked-add idx# dst-offset#)
                    (datatype->cast-fn ~src-dtype ~dst-dtype
                                       (.get src# (unchecked-add
                                                   idx# src-offset#))))))))


(defmacro update-base-copy-table
  []
  `(vector
    ~@(for [src-dtype datatypes
            dst-dtype datatypes
            unchecked? [false true]]
        `(do (base/add-copy-operation
              :nio-buffer :nio-buffer
              ~src-dtype ~dst-dtype ~unchecked?
              (fn [src# src-offset# dst# dst-offset# n-elems# options#]
                (buffer-buffer-copy ~src-dtype ~dst-dtype ~unchecked?
                                    src# src-offset# dst# dst-offset#
                                    n-elems# options#)))
             [:nio-buffer :nio-buffer ~src-dtype ~dst-dtype ~unchecked?]))))


(def core-copy-operations (update-base-copy-table))



(defmacro implement-scalar-primitive
  [cls datatype]
  `(clojure.core/extend
       ~cls
     base/PDatatype
     {:get-datatype (fn [item#] ~datatype)}
     base/PAccess
     {:get-value (fn [item# off#]
                   (when-not (= off# 0)
                     (throw (ex-info "Index out of range" {:offset off#})))
                   item#)}
     mp/PElementCount
     {:element-count (fn [item#] 1)}))


(implement-scalar-primitive Byte :int8)
(implement-scalar-primitive Short :int16)
(implement-scalar-primitive Integer :int32)
(implement-scalar-primitive Long :int64)
(implement-scalar-primitive Float :float32)
(implement-scalar-primitive Double :float64)
