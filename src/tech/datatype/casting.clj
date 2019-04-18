(ns tech.datatype.casting
  (:refer-clojure :exclude [cast])
  (:require [clojure.set :as c-set]))


(def signed-unsigned
  {:int8 :uint8
   :int16 :uint16
   :int32 :uint32
   :int64 :uint64})

(def unsigned-signed (c-set/map-invert signed-unsigned))

(def float-types #{:float32 :float64})

(def int-types (set (flatten (seq signed-unsigned))))

(def signed-int-types (set (keys signed-unsigned)))

(def unsigned-int-types (set (vals signed-unsigned)))

(def host-numeric-types (set (concat signed-int-types float-types)))

(def numeric-types (set (concat host-numeric-types unsigned-int-types)))

(def primitive-types (set (concat numeric-types [:boolean])))


(def base-host-datatypes (set (concat host-numeric-types
                                      [:object :boolean])))

(def base-datatypes (set (concat host-numeric-types
                                 unsigned-int-types
                                 [:boolean :object])))

(defn int-width
  ^long [dtype]
  (long (get {:int8 8
              :uint8 8
              :int16 16
              :uint16 16
              :int32 32
              :uint32 32
              :int64 64
              :uint64 64}
             dtype
             0)))

(defn float-width
  ^long [dtype]
  (long (get {:float32 32
              :float64 64}
             dtype
             0)))

(defn numeric-byte-width
  ^long [dtype]
  (long (cond
          (int-types dtype)
          (quot (int-width dtype) 8)
          (float-types dtype)
          (quot (float-width dtype) 8)
          :else
          (throw (ex-info (format "datatype is not numeric: %s" dtype)
                          {:datatype dtype})))))

(defn numeric-type?
  [dtype]
  (boolean
   (or (int-types dtype)
       (float-types dtype))))

(defn float-type?
  [dtype]
  (boolean
   (float-types dtype)))


(defn integer-type?
  [dtype]
  (boolean (int-types dtype)))


(defn signed-integer-type?
  [dtype]
  (boolean (contains? signed-unsigned dtype)))


(defn unsigned-integer-type?
  [dtype]
  (boolean (contains? unsigned-signed dtype)))


(defn integer-datatype->float-datatype
  [dtype]
  (if (#{:int8 :uint8 :int16 :uint16} dtype)
    :float32
    :float64))


(defn is-host-numeric-datatype?
  [dtype]
  (boolean (host-numeric-types dtype)))


(defn is-host-datatype?
  [dtype]
  (boolean (base-datatypes dtype)))


(defn datatype->host-datatype
  [dtype]
  (get unsigned-signed dtype dtype))


(defmacro bool->number
  [item]
  `(if ~item 1 0))


(defn ->number
  [item]
  (cond
    (number? item) item
    (boolean? item) (bool->number item)
    :else ;;punt!!
    (double item)))


(defmacro datatype->number
  [src-dtype item]
  (if-not (numeric-type? src-dtype)
    `(->number ~item)
    `~item))


(defmacro datatype->boolean
  [src-dtype item]
  (cond
    (numeric-type? src-dtype)
    `(boolean (not= 0.0 (unchecked-double ~item)))
    (= :boolean src-dtype)
    `(boolean ~item)
    :else
    `(boolean
      (if (number? ~item)
        (not= 0.0 (unchecked-double ~item))
        ~item))))

;; Save these because we are switching to unchecked soon.

(def int8-cast #(byte (->number %)))
(def int16-cast #(short (->number %)))
(def int32-cast #(int (->number %)))
(def int64-cast #(long (->number %)))
(def float32-cast #(float (->number %)))
(def float64-cast #(double (->number %)))
(def boolean-cast #(boolean (bool->number %)))

;;Numeric casts
(defmacro datatype->unchecked-cast-fn
  [src-dtype dtype val]
  (if (= src-dtype dtype)
    val
    (case dtype
      :int8 `(unchecked-byte (datatype->number ~src-dtype ~val))
      :int16 `(unchecked-short (datatype->number ~src-dtype ~val))
      :int32 `(unchecked-int (datatype->number ~src-dtype ~val))
      :int64 `(unchecked-long (datatype->number ~src-dtype ~val))

      :uint8 `(bit-and (unchecked-short (datatype->number ~src-dtype ~val)) 0xFF)
      :uint16 `(bit-and (unchecked-int (datatype->number ~src-dtype ~val)) 0xFFFF)
      :uint32 `(bit-and (unchecked-long (datatype->number ~src-dtype ~val)) 0xFFFFFFFF)
      :uint64 `(unchecked-long (datatype->number ~src-dtype ~val))

      :float32 `(unchecked-float (datatype->number ~src-dtype ~val))
      :float64 `(unchecked-double (datatype->number ~src-dtype ~val))
      :boolean `(datatype->boolean ~src-dtype ~val)
      :object `~val)))


(defmacro check
  [compile-time-max compile-time-min runtime-val datatype]
  `(if (or (> ~runtime-val
                ~compile-time-max)
             (< ~runtime-val
                ~compile-time-min))
     (throw (ex-info (format "Value out of range for %s: %s"
                             (name ~datatype) ~runtime-val)
                     {:min ~compile-time-min
                      :max ~compile-time-max
                      :value ~runtime-val}))
     ~runtime-val))


(defmacro datatype->cast-fn
  [src-dtype dst-dtype val]
  (if (= src-dtype dst-dtype)
    val
    (case dst-dtype
      :uint8 `(datatype->unchecked-cast-fn ~src-dtype ~dst-dtype
                                           (check (short 0xff) (short 0)
                                                  (short (datatype->number ~src-dtype
                                                                           ~val))
                                                  ~dst-dtype))
      :uint16 `(datatype->unchecked-cast-fn ~src-dtype ~dst-dtype
                                            (check (int 0xffff) (int 0)
                                                   (int (datatype->number ~src-dtype
                                                                          ~val))
                                                   ~dst-dtype))
      :uint32 `(datatype->unchecked-cast-fn ~src-dtype ~dst-dtype
                                            (check (long 0xffffffff) (int 0)
                                                   (long (datatype->number ~src-dtype
                                                                           ~val))
                                                   ~dst-dtype))
      :uint64 `(datatype->unchecked-cast-fn ~src-dtype ~dst-dtype
                                            (check (long Long/MAX_VALUE) (long 0)
                                                   (long (datatype->number ~src-dtype
                                                                           ~val))
                                                   ~dst-dtype))
      :int8 `(unchecked-byte (int8-cast ~val))
      :int16 `(unchecked-short (int16-cast ~val))
      :int32 `(unchecked-int (int32-cast ~val))
      :int64 `(unchecked-long (int64-cast ~val))
      :float32 `(unchecked-float (float32-cast ~val))
      :float64 `(unchecked-double (float64-cast ~val))
      :boolean `(datatype->boolean ~src-dtype ~val)
      :object `~val)))


(defonce ^:dynamic *cast-table* (atom {}))
(defonce ^:dynamic *unchecked-cast-table* (atom {}))


(defn add-cast-fn
  [datatype cast-fn]
  (swap! *cast-table* assoc datatype cast-fn))


(defn add-unchecked-cast-fn
  [datatype cast-fn]
  (swap! *unchecked-cast-table* assoc datatype cast-fn))


(defn cast
  [value datatype]
  (if-let [cast-fn (@*cast-table* datatype)]
    (cast-fn value)
    (throw (ex-info "No cast available" {:datatype datatype}))))


(defn unchecked-cast
  [value datatype]
  (if-let [cast-fn (@*unchecked-cast-table* datatype)]
    (cast-fn value)
    (throw (ex-info "No unchecked-cast available" {:datatype datatype}))))

(defmacro add-all-cast-fns
  []
  `(do
     ~@(for [dtype base-datatypes]
         [`(add-cast-fn ~dtype #(datatype->cast-fn :unknown ~dtype %))
          `(add-unchecked-cast-fn ~dtype #(datatype->unchecked-cast-fn
                                           :unkown ~dtype %))])))

(def casts (add-all-cast-fns))


(defn all-datatypes
  []
  (keys @*cast-table*))

(def all-host-datatypes
  (set (concat host-numeric-types
               [:boolean :object])))


(defn datatype->host-type
  "Get the signed analog of an unsigned type or return datatype unchanged."
  [datatype]
  (get unsigned-signed datatype datatype))


(defn jvm-cast
  [value datatype]
  (unchecked-cast value (datatype->host-type datatype)))


(defn datatype->safe-host-type
  "Get a jvm datatype wide enough to store all values of this datatype"
  [dtype]
  (case dtype
    :uint8 :int16
    :uint16 :int32
    :uint32 :int64
    :uint64 :int64
    dtype))

(defmacro datatype->host-cast-fn
  [src-dtype dst-dtype val]
  (let [host-type (datatype->host-type dst-dtype)]
    `(datatype->unchecked-cast-fn
      :ignored ~host-type
      (datatype->cast-fn ~src-dtype ~dst-dtype ~val))))


(defmacro datatype->unchecked-host-cast-fn
  [src-dtype dst-dtype val]
  (let [host-type (datatype->host-type dst-dtype)]
    `(datatype->unchecked-cast-fn
      :ignored ~host-type
      (datatype->unchecked-cast-fn ~src-dtype ~dst-dtype ~val))))


(defn flatten-datatype
  "Move a datatype into the canonical set"
  [dtype]
  (if (base-datatypes dtype)
    dtype
    :object))


(defn safe-flatten
  [dtype]
  (-> dtype
      datatype->safe-host-type
      flatten-datatype))


(defn host-flatten
  [dtype]
  (-> dtype
      datatype->host-datatype
      flatten-datatype))


(defmacro datatype->sparse-value
  [datatype]
  (cond
    (= datatype :object)
    `nil
    (= datatype :boolean)
    `false
    :else
    `(datatype->unchecked-cast-fn :unknown ~datatype 0)))
