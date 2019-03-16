(ns tech.datatype.java-primitive
  "Java specific mappings of the datatype system."
  (:require [tech.datatype.base-macros :as base-macros]
            [tech.datatype.base :as base]
            [clojure.core.matrix.macros :refer [c-for]]
            [clojure.core.matrix.protocols :as mp]
            [tech.jna :as jna])
  (:import [java.nio ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer Buffer]
           [com.sun.jna Pointer]
           [mikera.arrayz INDArray]
           [tech.datatype ByteConverter ShortConverter IntConverter
            LongConverter FloatConverter DoubleConverter]
           [java.lang.reflect Constructor]))


(set! *warn-on-reflection* true)


(defn ensure-ptr-like
  "JNA is extremely flexible in what it can take as an argument.  Anything convertible
  to a nio buffer, be it direct or array backend is fine."
  [item]
  (cond
    (satisfies? jna/PToPtr item)
    (jna/->ptr-backing-store item)
    :else
    (->buffer-backing-store item)))


;;JNA functions
(jna/def-jna-fn "c" memset
  "Set a block of memory to a value"
  Pointer
  [data ensure-ptr-like]
  [val int]
  [num-bytes int])


(jna/def-jna-fn "c" memcpy
  "Copy bytes from one object to another"
  Pointer
  [dst ensure-ptr-like]
  [src ensure-ptr-like]
  [n-bytes int])


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
      (->buffer-backing-store src)))
  base/PAccess
  (get-value [item idx]
    (cond
      (or (map? item)
          (vector? item))
      (do
        (when-not (contains? item idx)
          (throw (ex-info "Item has no idx entry"
                          {:item item
                           :idx idx})))
        (item idx))
      (fn? item)
      (item idx)
      :else
      (do
        (when-not (= 0 idx)
          (throw (ex-info "Generic index access must be 0"
                          {:item item
                           :idx idx})))
        item)))
  base/PClone
  (clone [item datatype]
    (base/copy! item (base/from-prototype item datatype
                                          (base/shape item)))))


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

(defmacro bool->number
  [item]
  `(if ~item 1 0))

;; Save these because we are switching to unchecked soon.
(defmacro ->number
  [item]
  `(if (not (number? ~item))
     (bool->number ~item)
     ~item))

(def int8-cast #(byte (->number %)))
(def int16-cast #(short (->number %)))
(def int32-cast #(int (->number %)))
(def int64-cast #(long (->number %)))
(def float32-cast #(float (->number %)))
(def float64-cast #(double (->number %)))


(base/add-cast-fn :int8 int8-cast)
(base/add-cast-fn :int16 int16-cast)
(base/add-cast-fn :int32 int32-cast)
(base/add-cast-fn :int64 int64-cast)
(base/add-cast-fn :float32 float32-cast)
(base/add-cast-fn :float64 float64-cast)


;; From this point on everything else is unchecked-checked!!
;; This is what allows this one file to provide both checked and unchecked operations.
;; The reason the rest of the file is unchecked is to provide faster iteration across
;; array access.
(set! *unchecked-math* :warn-on-boxed)


(base/add-unchecked-cast-fn :int8 #(unchecked-byte (->number %)))
(base/add-unchecked-cast-fn :int16 #(unchecked-short (->number %)))
(base/add-unchecked-cast-fn :int32 #(unchecked-int (->number %)))
(base/add-unchecked-cast-fn :int64 #(unchecked-long (->number %)))
(base/add-unchecked-cast-fn :float32 #(unchecked-float (->number %)))
(base/add-unchecked-cast-fn :float64 #(unchecked-double (->number %)))


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
      :int8 `(unchecked-byte (->number ~val))
      :int16 `(unchecked-short (->number ~val))
      :int32 `(unchecked-int (->number ~val))
      :int64 `(unchecked-long (->number ~val))
      :float32 `(unchecked-float (->number ~val))
      :float64 `(unchecked-double (->number ~val)))))


(defmacro datatype->buffer-creation
  [datatype src-ary]
  (case datatype
    :int8 `(ByteBuffer/wrap ^bytes ~src-ary)
    :int16 `(ShortBuffer/wrap ^shorts ~src-ary)
    :int32 `(IntBuffer/wrap ^ints ~src-ary)
    :int64 `(LongBuffer/wrap ^longs ~src-ary)
    :float32 `(FloatBuffer/wrap ^floats ~src-ary)
    :float64 `(DoubleBuffer/wrap ^doubles ~src-ary)))


(defonce ^:dynamic *array-constructors* (atom {}))


(defn add-array-constructor!
  [item-dtype cons-fn]
  (swap! *array-constructors* assoc item-dtype cons-fn)
  (keys @*array-constructors*))


(defn add-numeric-array-constructor
  [item-dtype ary-cons-fn]
  (add-array-constructor!
   item-dtype
   (fn [elem-count-or-seq options]
     (cond
       (number? elem-count-or-seq)
       (ary-cons-fn elem-count-or-seq)
       (satisfies? base/PDatatype elem-count-or-seq)
       (if (and (satisfies? PToArray elem-count-or-seq)
                (= item-dtype (base/get-datatype elem-count-or-seq)))
         (->array-copy elem-count-or-seq)
         (let [n-elems (base/ecount elem-count-or-seq)]
           (base/copy! elem-count-or-seq 0
                       (ary-cons-fn n-elems) 0
                       n-elems options)))
       :else
       (let [elem-count-or-seq (if (or (number? elem-count-or-seq)
                                       (:unchecked? options))
                                 elem-count-or-seq
                                 (map #(base/cast % item-dtype) elem-count-or-seq))]
         (ary-cons-fn elem-count-or-seq))))))


(add-numeric-array-constructor :int8 byte-array)
(add-numeric-array-constructor :int16 short-array)
(add-numeric-array-constructor :int32 int-array)
(add-numeric-array-constructor :int64 long-array)
(add-numeric-array-constructor :float32 float-array)
(add-numeric-array-constructor :float64 double-array)
(add-numeric-array-constructor :boolean boolean-array)


(defn make-object-array-of-type
  [obj-type elem-count-or-seq options]
  (let [elem-count-or-seq (if (or (number? elem-count-or-seq)
                                     (:unchecked? options))
                               elem-count-or-seq
                               (map (partial jna/ensure-type obj-type)
                                    elem-count-or-seq))]
    (if (number? elem-count-or-seq)
      (let [constructor (if (:construct? options)
                          (.getConstructor ^Class obj-type (make-array Class 0))
                          nil)]
        (if constructor
          (into-array obj-type (repeatedly (long elem-count-or-seq)
                                           #(.newInstance
                                             ^Constructor constructor
                                             (make-array Object 0))))
          (make-array obj-type (long elem-count-or-seq))))
      (into-array obj-type elem-count-or-seq))))


(defn make-array-of-type
  ([datatype elem-count-or-seq options]
   (if (instance? Class datatype)
     (make-object-array-of-type datatype elem-count-or-seq options)
     (if-let [cons-fn (get @*array-constructors* datatype)]
       (cons-fn elem-count-or-seq options)
       (throw (ex-info (format "Failed to find constructor for datatype %s" datatype)
                       {:datatype datatype})))))
  ([datatype elem-count-or-seq]
   (make-array-of-type datatype elem-count-or-seq {})))


(base/add-cast-fn :string str)
(base/add-unchecked-cast-fn :string str)

(add-numeric-array-constructor :string #(make-object-array-of-type String % {:construct? true}))


(defn make-buffer-of-type
  ([datatype elem-count-or-seq options]
   (->buffer-backing-store
    (make-array-of-type datatype elem-count-or-seq options)))
  ([datatype elem-count-or-seq]
   (make-buffer-of-type datatype elem-count-or-seq {})))


(defn memset-constant
  "Try to memset a constant value.  Returns true if succeeds, false otherwise"
  [item offset value elem-count]
  (let [offset (long offset)
        elem-count (long elem-count)]
    (if (or (= 0.0 (double value))
            (and (<= Byte/MAX_VALUE (long value))
                 (>= Byte/MIN_VALUE (long value))
                 (= :int8 (base/get-datatype item))))
      (do
        (when-not (<= (+ (long offset)
                         (long elem-count))
                      (base/ecount item))
          (throw (ex-info "Memset out of range"
                          {:offset offset
                           :elem-count elem-count
                           :item-ecount (base/ecount item)})))
        (memset (offset-item item offset) (int value)
                (* elem-count (base/datatype->byte-size
                               (base/get-datatype item))))
        true)
      false)))


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
                          (datatype->cast-fn :ignored ~datatype value#))
                    item#)
      :set-constant! (fn [item# ^long offset# value# ^long elem-count#]
                       (let [value# (datatype->cast-fn :ignored ~datatype value#)
                             item# (datatype->array-cast-fn ~datatype item#)
                             offset# (long offset#)
                             off-elem-count# (+ (long elem-count#)
                                            offset#)]
                         (when-not (memset-constant item# offset# value# elem-count#)
                           (c-for [idx# offset# (< idx# off-elem-count#) (+ idx# 1)]
                                  (aset item# idx# value#)))
                         item#))}
     base/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (let [copy-len# (alength (datatype->array-cast-fn ~datatype
                                                                           raw-data#))]
                           (base/copy! raw-data# 0 ary-target# target-offset#
                                       copy-len# options#)
                           [ary-target# (+ (long target-offset#) copy-len#)]))}
     base/PPersistentVector
     {:->vector (fn [src-ary#] (vec src-ary#))}
     base/PPrototype
     {:from-prototype (fn [src-ary# datatype# shape#]
                        (make-array-of-type datatype# (base/shape->ecount shape#)))}
     PToBuffer
     {:->buffer-backing-store (fn [src-ary#]
                                (datatype->buffer-creation ~datatype src-ary#))}
     POffsetable
     {:offset-item (fn [src-ary# offset#]
                     (offset-item (->buffer-backing-store src-ary#) offset#))}
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


(defn world->bool
  [item]
  (if (number? item)
    (not= 0.0 (double item))
    (boolean item)))

(defn bool->world
  ^long [bool-val]
  (if (boolean bool-val)
    1
    0))

(base/add-cast-fn :boolean world->bool)
(base/add-unchecked-cast-fn :boolean world->bool)


(defn as-boolean-array
  ^"[Z" [obj] obj)

(extend-type (Class/forName "[Z")
  base/PDatatype
  (get-datatype [_] :boolean)

  base/PContainerType
  (container-type [_] :boolean-array)

  base/PAccess
  (get-value [item idx]
    (aget (as-boolean-array item) (int idx)))
  (set-value! [item idx value]
    (aset (as-boolean-array item) (int idx) ^boolean (world->bool value))
    item)
  (set-constant! [item offset value elem-count]
    (let [offset (long offset)
          off-elem-count (+ (long elem-count) offset)
          value (boolean value)
          item (as-boolean-array item)]
      (c-for [idx offset (< idx off-elem-count) (unchecked-add idx 1)]
             (aset item idx value))
      item))

  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target offset options]
    (let [raw-data (as-boolean-array raw-data)
          elem-count (alength raw-data)
          offset (long offset)
          off-elem-count (+ (long elem-count) offset)
          target-dtype (base/get-datatype ary-target)]
      (c-for [idx offset (< idx off-elem-count) (unchecked-add idx 1)]
             (base/set-value! ary-target idx (if (aget raw-data (- idx offset))
                                               0
                                               1)))
      [ary-target (+ offset off-elem-count)]))

  base/PPersistentVector
  (->vector [item] (vec item))

  base/PPrototype
  (from-prototype [src-ary datatype shape]
    (make-array-of-type datatype (base/shape->ecount shape)))


  PToArray
  (->array [item] item)
  (->array-copy [src-ary]
    (base/copy! src-ary (make-array-of-type :boolean (alength (as-boolean-array src-ary))))))


(defn as-object-array
  ^"[Ljava.lang.Object;" [item]
  item)


(defonce ^:dynamic *object-array-datatype-override* (atom nil))


(defn add-object-array-datatype-override!
  [cls-dtype override-val]
  (swap! *object-array-datatype-override* assoc cls-dtype override-val)
  (keys @*object-array-datatype-override*))


(defn extend-object-array-type
  [obj-ary-cls]
  (when-not (.isArray ^Class obj-ary-cls)
    (throw (ex-info "Obj class is not an array class" {})))

  (clojure.core/extend
      obj-ary-cls
    base/PDatatype
    {:get-datatype (fn [item]
                     (let [ary-data-cls (.getComponentType ^Class (type item))]
                       (get @*object-array-datatype-override* ary-data-cls ary-data-cls)))}

    base/PContainerType
    {:container-type (fn [item] (type item))}

    base/PAccess
    {:get-value (fn [item idx]
                  (aget (as-object-array item) (int idx)))
     :set-value! (fn [item idx value]
                   (when-not (jna/ensure-type (.getComponentType ^Class (type item))
                                              value)
                     (throw (ex-info "Value is not of expected array type."
                                     {:datatype (base/get-datatype item)
                                      :value-type (type value)})))
                   (aset (as-object-array item) (int idx) value)
                   item)
     :set-constant! (fn [item offset value elem-count]
                      (when-not (jna/ensure-type (.getComponentType ^Class (type item))
                                                 value)
                        (throw (ex-info "Value is not of expected array type."
                                        {:datatype (base/get-datatype item)
                                         :value-type (type value)})))
                      (let [offset (long offset)
                            off-elem-count (+ (long elem-count) offset)
                            item (as-object-array item)]
                        (c-for [idx offset (< idx off-elem-count) (unchecked-add idx 1)]
                               (aset item idx value))
                        item))}

  base/PCopyRawData
  {:copy-raw->item! (fn [raw-data ary-target offset options]
                      (let [raw-data (as-object-array raw-data)
                            elem-count (alength raw-data)
                            offset (long offset)
                            off-elem-count (+ (long elem-count) offset)
                            target-dtype (base/get-datatype ary-target)]
                        (c-for [idx offset (< idx off-elem-count) (unchecked-add idx 1)]
                               (base/set-value! ary-target idx (aget raw-data (- idx offset))))
                        [ary-target (+ offset off-elem-count)]))}

  base/PPersistentVector
  {:->vector (fn [item] (vec item))}

  base/PPrototype
  {:from-prototype (fn [src-ary datatype shape]
                     (make-array-of-type datatype (base/shape->ecount shape)))}

  PToArray
  {:->array (fn [item] item)
   :->array-copy (fn [src-ary]
                   (base/copy! src-ary (make-array-of-type :boolean (alength (as-boolean-array src-ary)))))}))


(extend-object-array-type (Class/forName "[Ljava.lang.Object;"))
(extend-object-array-type (Class/forName "[Ljava.lang.String;"))
(add-object-array-datatype-override! String :string)


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
                            (datatype->cast-fn :ignored ~datatype value#))
                      item#))
      :set-constant! (fn [item# ^long offset# value# ^long elem-count#]
                       (let [value# (datatype->cast-fn :ignored ~datatype value#)
                             item# (datatype->buffer-cast-fn ~datatype item#)
                             offset# (long offset#)
                             off-elem-count# (+ (long elem-count#)
                                                offset#)]
                         (when-not (memset-constant item# offset# value# elem-count#)
                           (c-for [idx# offset# (< idx# off-elem-count#) (+ idx# 1)]
                                  (.put item# (+ idx# (.position item#)) value#))))
                       item#)}
     base/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (let [copy-len# (.remaining (datatype->buffer-cast-fn
                                                      ~datatype raw-data#))]
                           (base/copy! raw-data# 0 ary-target# target-offset#
                                       copy-len# options#)
                           [ary-target# (+ (long target-offset#) copy-len#)]))}
     base/PPrototype
     {:from-prototype (fn [src-ary# datatype# shape#]
                        (if-not (.isDirect (datatype->buffer-cast-fn ~datatype src-ary#))
                          (make-buffer-of-type datatype# (base/shape->ecount shape#))
                          (throw (ex-info "Cannot clone direct nio buffers" {}))))}
     PToBuffer
     {:->buffer-backing-store (fn [item#] item#)}
     POffsetable
     {:offset-item (fn [src-buf# offset#]
                     (let [src-buf# (datatype->buffer-cast-fn ~datatype src-buf#)
                           src-buf# (.slice src-buf#)
                           offset# (long offset#)]
                       (when-not (<= offset# (base/ecount src-buf#))
                         (throw (ex-info "Offset out of range:"
                                         {:offset offset#
                                          :ecount (base/ecount src-buf#)})))
                       (.position src-buf# (+ (long offset#)
                                              (.position src-buf#)))
                       src-buf#))}
     PToArray
     {:->array (fn [item#]
                 (let [item# (datatype->buffer-cast-fn ~datatype item#)]
                   (when (and (= 0 (.position item#))
                              (not (.isDirect item#)))
                     (let [array-data# (.array item#)]
                       (when (= (.limit item#)
                                (alength array-data#))
                         array-data#)))))
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


(defn byte-converter-cast ^ByteConverter [item] item)
(defn short-converter-cast ^ShortConverter [item] item)
(defn int-converter-cast ^IntConverter [item] item)
(defn long-converter-cast ^LongConverter [item] item)
(defn float-converter-cast ^FloatConverter [item] item)
(defn double-converter-cast ^DoubleConverter [item] item)


(defmacro datatype->converter
  [datatype item]
  (case datatype
    :int8 `(byte-converter-cast ~item)
    :int16 `(short-converter-cast ~item)
    :int32 `(int-converter-cast ~item)
    :int64 `(long-converter-cast ~item)
    :float32 `(float-converter-cast ~item)
    :float64 `(double-converter-cast ~item)))


(defmacro buffer-converter-copy-fn
  [dst-dtype]
  `(fn [dst# dst-offset# n-elems# converter#]
     (let [n-elems# (int n-elems#)
           dst-buf# (datatype->buffer-cast-fn ~dst-dtype (->buffer-backing-store dst#))
           dst# (datatype->array-cast-fn ~dst-dtype (->array dst#))
           dst-offset# (+ (.position dst-buf#) (int dst-offset#))
           converter# (datatype->converter ~dst-dtype converter#)]
       (if dst#
         (c-for [idx# (int 0) (< idx# n-elems#) (unchecked-add idx# 1)]
                (aset dst# (unchecked-add idx# dst-offset#)
                      (.convert converter# idx#)))
         (let [dst# dst-buf#]
           (c-for [idx# (int 0) (< idx# n-elems#) (unchecked-add idx# 1)]
                  (.put dst# (unchecked-add idx# dst-offset#)
                        (.convert converter# idx#))))))))


(defmacro create-converter-fns
  []
  (->> (for [dtype datatypes]
         [dtype `(buffer-converter-copy-fn ~dtype)])
       (into {})))


(def converter-fn-map (create-converter-fns))


(defmacro make-converter
  [dst-dtype & convert-code]
  (case dst-dtype
    :int8 `(reify ByteConverter
             (convert [this# ~'idx]
               ~@convert-code))
    :int16 `(reify ShortConverter
              (convert [this# ~'idx]
                ~@convert-code))
    :int32 `(reify IntConverter
              (convert [this# ~'idx]
                ~@convert-code))
    :int64 `(reify LongConverter
              (convert [this# ~'idx]
                ~@convert-code))
    :float32 `(reify FloatConverter
                (convert [this# ~'idx]
                  ~@convert-code))
    :float64 `(reify DoubleConverter
                (convert [this# ~'idx]
                  ~@convert-code))))


(defmacro buffer-buffer-copy
  [src-dtype dst-dtype unchecked? src src-offset dst dst-offset n-elems options]
  (cond
    ;;If datatypes are equal, then just use memcpy
    (= src-dtype dst-dtype)
    `(memcpy (offset-item ~dst ~dst-offset)
             (offset-item ~src ~src-offset)
             (* (long ~n-elems)
                (base/datatype->byte-size ~src-dtype)))
    unchecked?
    `(let [src-buf# (datatype->buffer-cast-fn ~src-dtype ~src)
           src# (datatype->array-cast-fn ~src-dtype (->array ~src))
           src-offset# (+ (.position src-buf#) (int ~src-offset))
           converter-fn# (get converter-fn-map ~dst-dtype)]
       ;;Fast path if both are representable by java arrays
       (if src#
         (converter-fn# ~dst ~dst-offset ~n-elems
                        (make-converter
                         ~dst-dtype
                         (datatype->unchecked-cast-fn
                          ~src-dtype ~dst-dtype
                          (aget src# (unchecked-add
                                      ~'idx src-offset#)))))
         (let [src# (datatype->buffer-cast-fn
                     ~src-dtype (->buffer-backing-store ~src))]
           (converter-fn# ~dst ~dst-offset ~n-elems
                          (make-converter
                           ~dst-dtype
                           (datatype->unchecked-cast-fn
                            ~src-dtype ~dst-dtype
                            (.get src# (unchecked-add
                                        ~'idx src-offset#))))))))
    :else
    `(let [src# (datatype->buffer-cast-fn ~src-dtype ~src)
           src-buf# (datatype->buffer-cast-fn ~src-dtype ~src)
           src-offset# (+ (.position src-buf#) (int ~src-offset))
           converter-fn# (get converter-fn-map ~dst-dtype)]
       (converter-fn# ~dst ~dst-offset ~n-elems
                      (make-converter
                       ~dst-dtype
                       (datatype->cast-fn
                        ~src-dtype ~dst-dtype
                        (.get src# (unchecked-add
                                    ~'idx src-offset#))))))))


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
