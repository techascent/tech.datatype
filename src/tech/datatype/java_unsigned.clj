(ns tech.datatype.unsigned
  "Adds unsigned integer support to nio buffers."
  (:require [tech.datatype.base :as base]
            [tech.datatype.java-primitive :as primitive]
            [clojure.core.matrix.protocols :as mp]
            [clojure.core.matrix.macros :refer [c-for]]
            [clojure.set :as c-set]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defprotocol PToTypedBuffer
  (->typed-buffer [item]))


;;We first instroduce a new type of cast and this takes the datatype and
;;casts it to it storage type in the jvm.


(def ^:dynamic *jvm-cast-table* (atom {}))
(def ^:dynamic *unchecked-jvm-cast-table* (atom {}))
(def ^:dynamic *->jvm-datatype-table* (atom {}))

(defn add-jvm-cast
  [dtype cast-fn]
  (swap! *jvm-cast-table* assoc dtype cast-fn))


(defn add-unchecked-jvm-cast
  [dtype cast-fn]
  (swap! *unchecked-jvm-cast-table* assoc dtype cast-fn))

(defn jvm-cast
  [dtype value]
  (if-let [cast-fn (get @*jvm-cast-table* dtype)]
    (cast-fn value)
    (throw (ex-info "Failed to find jvm cast"
                    {:datatype dtype}))))


(defn unchecked-jvm-cast
  [dtype value]
    (if-let [cast-fn (get @*unchecked-jvm-cast-table* dtype)]
    (cast-fn value)
    (throw (ex-info "Failed to find jvm cast"
                    {:datatype dtype}))))


(defn is-jvm-datatype?
  [dtype]
  (boolean ((set primitive/datatypes) dtype)))


(defn add-datatype->jvm-datatype-conversion
  [src-dtype dst-dtype]
  (when-not (is-jvm-datatype? dst-dtype)
    (throw (ex-info "Destination datatype is not a jvm datatype"
                    {:dst-dtype dst-dtype})))
  (swap! *->jvm-datatype-table* assoc src-dtype dst-dtype))



(base/add-datatype->size-mapping :uint8 1)
(base/add-datatype->size-mapping :uint16 2)
(base/add-datatype->size-mapping :uint32 4)
(base/add-datatype->size-mapping :uint64 8)

;;The unsigned types have to cast directly to their signed types
;;and vice versa in all cases.
(def direct-unsigned->signed-map {:uint8 :int8
                                  :uint16 :int16
                                  :uint32 :int32
                                  :uint64 :int64})


(def direct-signed->unsigned-map (c-set/map-invert direct-unsigned->signed-map))


(def unsigned-datatypes (set (keys direct-unsigned->signed-map)))


(doseq [[u-dtype s-dtype] direct-unsigned->signed-map]
  (add-datatype->jvm-datatype-conversion u-dtype s-dtype))


(defn datatype->jvm-datatype
  [src-dtype]
  (if (is-jvm-datatype? src-dtype)
    src-dtype
    (if-let [retval (@*->jvm-datatype-table* src-dtype)]
      retval
      (throw (ex-info "Unknown conversion to jvm datatype"
                      {:src-dtype src-dtype})))))



(defn unsigned-datatype?
  [dtype]
  (boolean (unsigned-datatypes dtype)))


(defn direct-conversion?
  [src-dtype dst-dtype]
  (or (= dst-dtype (direct-unsigned->signed-map src-dtype))
      (= dst-dtype (direct-signed->unsigned-map src-dtype))))


(defmacro datatype->unsigned-max
  [datatype]
  (case datatype
    :uint8 (short 0xFF)
    :uint16 (int 0xFFFF)
    :uint32 (long 0xFFFFFFFF)
    :uint64 Long/MAX_VALUE))


(defmacro check
  [compile-time-max compile-time-min runtime-val]
  `(if (or (> ~runtime-val
                ~compile-time-max)
             (< ~runtime-val
                ~compile-time-min))
     (throw (ex-info "Value out of range"
                     {:min ~compile-time-min
                      :max ~compile-time-max
                      :value ~runtime-val}))
     ~runtime-val))


(defmacro datatype->unchecked-cast-fn
  [src-dtype dst-dtype val]
  (if (= src-dtype dst-dtype)
    val
    (case dst-dtype
      :uint8 `(bit-and (unchecked-short ~val) 0xFF)
      :uint16 `(bit-and (unchecked-int ~val) 0xFFFF)
      :uint32 `(bit-and (unchecked-long ~val) 0xFFFFFFFF)
      :uint64 `(unchecked-long ~val)
      `(primitive/datatype->unchecked-cast-fn ~src-dtype ~dst-dtype ~val))))


(defmacro datatype->cast-fn
  [src-dtype dst-dtype val]
  (if (or (= src-dtype dst-dtype)
          (direct-conversion? src-dtype dst-dtype))
    val
    (case dst-dtype
      :uint8 `(datatype->unchecked-cast-fn ~src-dtype ~dst-dtype (check (short 0xff) (short 0) (short ~val)))
      :uint16 `(datatype->unchecked-cast-fn ~src-dtype ~dst-dtype (check (int 0xffff) (int 0) (int ~val)))
      :uint32 `(datatype->unchecked-cast-fn ~src-dtype ~dst-dtype (check (long 0xffffffff) (int 0) (long ~val)))
      :uint64 `(datatype->unchecked-cast-fn ~src-dtype ~dst-dtype ~val)
      `(primitive/datatype->cast-fn ~src-dtype ~dst-dtype ~val))))


(defmacro datatype->jvm-cast-fn
  [src-dtype dst-dtype val]
  (let [jvm-type (datatype->jvm-datatype dst-dtype)]
    `(primitive/datatype->cast-fn :ignored
                                  ~jvm-type
                                  (datatype->cast-fn ~src-dtype ~dst-dtype ~val))))


(defmacro datatype->unchecked-jvm-cast-fn
  [src-dtype dst-dtype val]
  (let [jvm-type (datatype->jvm-datatype dst-dtype)]
    `(primitive/datatype->cast-fn :ignored
                                  ~jvm-type
                                  (datatype->unchecked-cast-fn ~src-dtype ~dst-dtype ~val))))


(defmacro casting
  []
  `(do
     ~@(for [u-dtype unsigned-datatypes]
         (let [s-dtype (direct-unsigned->signed-map u-dtype)]
           `(do
              (base/add-cast-fn ~u-dtype (fn [val#] (datatype->cast-fn :ignored ~u-dtype val#)))
              (base/add-unchecked-cast-fn ~u-dtype (fn [val#] (datatype->unchecked-cast-fn :ignored ~u-dtype val#)))
              (add-jvm-cast ~u-dtype (fn [val#] (datatype->jvm-cast-fn :ignored ~u-dtype val#)))
              (add-unchecked-jvm-cast ~u-dtype (fn [val#] (datatype->unchecked-jvm-cast-fn :ignored ~u-dtype val#))))))))


(def casts (casting))


(defrecord TypedBuffer [buffer dtype]
  base/PDatatype
  (get-datatype [item] dtype)
  mp/PElementCount
  (element-count [item] (mp/element-count buffer))
  base/PAccess
  (set-value! [item offset value]
    (base/set-value! (primitive/->buffer item) offset (jvm-cast dtype value)))
  (set-constant! [item offset value elem-count]
    (base/set-constant! (primitive/->buffer item) offset (jvm-cast dtype value) elem-count))
  (get-value [item offset]
    (-> (base/get-value (primitive/->buffer item) offset)
        (base/unchecked-cast dtype)))
  base/PContainerType
  (container-type [_] :typed-buffer)
  base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (primitive/raw-dtype-copy! raw-data ary-target target-offset options))
  primitive/PToBuffer
  (->buffer [item] (primitive/->buffer buffer)))


(extend-type Object
  PToTypedBuffer
  (->typed-buffer [item]
    (->TypedBuffer (primitive/->buffer item) (base/get-datatype item))))


;;And now we fill out the copy table.
;;All the direct conversions can use a ->buffer pathway both to and from the container for
;;unchecked.


(base/add-container-conversion-fn :java-array :typed-buffer (fn [dst-dtype src-data]
                                                              [(->typed-buffer src-data) 0]))
(base/add-container-conversion-fn :nio-buffer :typed-buffer (fn [dst-dtype src-data]
                                                              [(->typed-buffer src-data) 0]))

(def all-possible-datatype-pairs
  (let [all-dtypes (concat primitive/datatypes unsigned-datatypes)]
    (->> (for [src-dtype all-dtypes
               dst-dtype all-dtypes]
           [src-dtype dst-dtype])
         set)))

(def trivial-conversions (->> all-possible-datatype-pairs
                              (filter (fn [[src-dtype dst-dtype]]
                                        (or (direct-conversion? src-dtype dst-dtype)
                                            (= src-dtype dst-dtype)
                                            (not (or (unsigned-datatype? src-dtype)
                                                     (unsigned-datatype? dst-dtype))))))))

(def nontrivial-conversions (c-set/difference (set all-possible-datatype-pairs)
                                              (set trivial-conversions)))

(defn- raw-copy
  [src src-offset dst dst-offset elem-count options]
  (base/copy! (primitive/->buffer src) src-offset
              (primitive/->buffer dst) dst-offset
              elem-count options))


(def raw-copy-operations
  (->> (for [[src-dtype dst-dtype] trivial-conversions
             unchecked? [true false]]
         (do
           (base/add-copy-operation :typed-buffer :typed-buffer src-dtype dst-dtype unchecked? raw-copy)
           [:typed-buffer :typed-buffer src-dtype dst-dtype unchecked?]))
       vec))


(defmacro bufferable-bufferable-copy
  [src-dtype dst-dtype unchecked?]
  (if unchecked?
    `(fn [src# src-offset# dst# dst-offset# elem-count# options#]
       (let [src# (primitive/datatype->buffer-cast-fn ~(datatype->jvm-datatype src-dtype) (primitive/->buffer src#))
             dst# (primitive/datatype->buffer-cast-fn ~(datatype->jvm-datatype dst-dtype) (primitive/->buffer dst#))
             src-offset# (long src-offset#)
             dst-offset# (long dst-offset#)
             elem-count# (long elem-count#)]
         (c-for [idx# 0 (< idx# elem-count#) (+ idx# 1)]
                (.put dst# (+ idx# dst-offset#)
                      (datatype->unchecked-jvm-cast-fn ~src-dtype ~dst-dtype
                                                       (.get src# (+ idx# dst-offset#)))))))
    `(fn [src# src-offset# dst# dst-offset# elem-count# options#]
       (let [src# (primitive/datatype->buffer-cast-fn ~(datatype->jvm-datatype src-dtype) (primitive/->buffer src#))
             dst# (primitive/datatype->buffer-cast-fn ~(datatype->jvm-datatype dst-dtype) (primitive/->buffer dst#))
             src-offset# (long src-offset#)
             dst-offset# (long dst-offset#)
             elem-count# (long elem-count#)]
         (c-for [idx# 0 (< idx# elem-count#) (+ idx# 1)]
                (.put dst# (+ idx# dst-offset#)
                      (datatype->jvm-cast-fn ~src-dtype ~dst-dtype
                                         (.get src# (+ idx# dst-offset#)))))))))


(defmacro custom-conversions-macro
  []
  `(vector
     ~@(for [[src-dtype dst-dtype] custom-convert-dtype-combos
             unchecked? [true false]]
         `(let [operation# (bufferable-bufferable-copy ~src-dtype ~dst-dtype ~unchecked?)]
            (base/add-copy-operation :typed-buffer :typed-buffer ~src-dtype ~dst-dtype ~unchecked?
                                     operation#)
            [:typed-buffer :typed-buffer ~src-dtype ~dst-dtype ~unchecked?]))))


(def custom-conversions (custom-conversions-macro))
