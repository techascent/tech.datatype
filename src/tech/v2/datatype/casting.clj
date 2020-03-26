(ns tech.v2.datatype.casting
  (:refer-clojure :exclude [cast])
  (:require [clojure.set :as c-set]
            [primitive-math :as pmath])
  (:import [tech.v2.datatype DateUtility]
           [java.util Map]
           [java.util.concurrent ConcurrentHashMap]))

(set! *warn-on-reflection* true)

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
  (long (get {:boolean 8 ;;unfortunately, this is what C decided
              :int8 8
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
(def boolean-cast #(datatype->boolean :unknown %))

;;Numeric casts
(defmacro datatype->unchecked-cast-fn
  [src-dtype dtype val]
  (if (= src-dtype dtype)
    val
    (case dtype
      :int8 `(pmath/byte (datatype->number ~src-dtype ~val))
      :int16 `(pmath/short (datatype->number ~src-dtype ~val))
      :int32 `(pmath/int (datatype->number ~src-dtype ~val))
      :int64 `(pmath/long (datatype->number ~src-dtype ~val))

      :uint8 `(pmath/byte->ubyte (datatype->number ~src-dtype ~val))
      :uint16 `(pmath/short->ushort (datatype->number ~src-dtype ~val))
      :uint32 `(pmath/int->uint (datatype->number ~src-dtype ~val))
      :uint64 `(pmath/long (datatype->number ~src-dtype ~val))

      :float32 `(pmath/float (datatype->number ~src-dtype ~val))
      :float64 `(pmath/double (datatype->number ~src-dtype ~val))
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
      :int8 `(pmath/byte (int8-cast ~val))
      :int16 `(pmath/short (int16-cast ~val))
      :int32 `(pmath/int (int32-cast ~val))
      :int64 `(pmath/long (int64-cast ~val))
      :float32 `(pmath/float (float32-cast ~val))
      :float64 `(pmath/double (float64-cast ~val))
      :boolean `(datatype->boolean ~src-dtype ~val)
      :keyword `(keyword ~val)
      :symbol `(symbol ~val)
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


(def date-denominators
  {:milliseconds 1
   :seconds 1000
   :minutes DateUtility/minutesToMillis
   :hours DateUtility/hoursToMillis
   :days DateUtility/daysToStandardMillis})


(def date-denominator-names (set (keys date-denominators)))


(defn valid-date-denominator?
  [denominator]
  (contains? date-denominator-names denominator))


(defn check-date-denominator
  [denominator]
  (if (number? denominator)
    denominator
    (do
      (when-not (valid-date-denominator? denominator)
        (throw (Exception. (format "Unrecognized date denominator: %s"
                                   denominator))))
      (get date-denominators denominator))))


(defn date-denominator
  ^long [denominator]
  (if-let [retval (get date-denominators denominator)]
    retval
    (throw (Exception. (format "Unrecognized date denominator: %s"
                               denominator)))))


(def valid-datetime-base-datatypes
  #{:int64})


(defn valid-datetime-base-datatype?
  [dtype]
  (contains? valid-datetime-base-datatypes dtype))


(defn check-date-base-datatype!
  [dtype]
  (when-not (valid-datetime-base-datatype? dtype)
    (throw (Exception. "Invalid date base datatype: %s"
                       dtype)))
  dtype)


(defn datetime-datatype
  ([date-denominator utc-offset base-datatype]
   {:datatype :datetime
    :base-datatype (check-date-base-datatype! base-datatype)
    :denominator (check-date-denominator date-denominator)
    :utc-offset utc-offset})
  ([date-denominator utc-offset]
   (datetime-datatype date-denominator utc-offset :int64))
  ([date-denominator]
   (datetime-datatype date-denominator 0))
  ([]
   (datetime-datatype :milliseconds)))


(defn datetime-datatype?
  [item]
  (= :datetime (:datatype item)))


(defn datetime-object-family?
  [item]
  (or (= :instant item)
      (= :local-date-time item)
      (= :zoned-date-time item)))

(defn datetime-family-class
  [item]
  (cond
    (datetime-datatype? item) :concrete
    (datetime-object-family? item) :object
    :else :out-of-family))


(defn datetime-family?
  [item]
  (not= :out-of-family (datetime-family-class item)))


(def time-denominators
  (->> (assoc date-denominators :nanoseconds 1E-6)
       (map (fn [[k v]]
              [k (double v)]))
       (into {})))


(def valid-time-denominators (set (keys time-denominators)))


(defn valid-timeinterval-denominator?
  [denominator]
  (contains? valid-time-denominators denominator))


(defn check-timeinterval-denominator!
  [denominator]
  (when-not (valid-timeinterval-denominator? denominator)
    (throw (Exception. (format  "Invalid timeinterval denominator: %s" denominator))))
  denominator)


(def valid-timeinterval-datatypes
  #{:int64})


(defn valid-timeinterval-base-datatype?
  [dtype]
  (contains? valid-timeinterval-datatypes dtype))


(defn check-timeinterval-base-datatype!
  [dtype]
  (when-not (valid-timeinterval-base-datatype? dtype)
    (throw (Exception. (format "Invalid time interval datatype: %s"
                               dtype))))
  dtype)


(defn timeinterval-datatype
  ([denominator base-datatype]
   {:datatype :timeinterval
    :base-datatype (check-timeinterval-base-datatype! base-datatype)
    :denominator (check-timeinterval-denominator! denominator)})
  ([date-denominator]
   (timeinterval-datatype date-denominator :int64))
  ([]
   (timeinterval-datatype :milliseconds)))


(defn timeinterval-datatype?
  [item]
  (= :timeinterval (:datatype item)))


(defn timeinterval-object-family?
  [item]
  (= :duration item))

(defn timeinterval-family-class
  [item]
  (cond
    (timeinterval-datatype? item) :concrete
    (timeinterval-object-family? item) :object
    :else :out-of-family))


(defn timeinterval-family?
  [item]
  (not= :out-of-family (timeinterval-family-class item)))


(defonce aliased-datatypes (ConcurrentHashMap.))

(defn alias-datatype!
  "Alias a new datatype to a base datatype.  Only useful for primitive datatypes"
  [new-dtype old-dtype]
  (.put ^Map aliased-datatypes new-dtype old-dtype))


(defn un-alias-datatype
  [dtype]
  (.getOrDefault ^Map aliased-datatypes dtype dtype))


(defn composite-datatype?
  [dtype]
  (and (instance? java.util.Map dtype)
       (.get ^java.util.Map dtype :base-datatype)
       (.containsKey ^Map aliased-datatypes dtype)))


(defn composite-datatype->base-datatype
  "If this is a composite datatype, return the base datatype.  Else return
  input unchanged."
  [dtype]
  (if (instance? java.util.Map dtype)
    (.get ^Map dtype :base-datatype)
    (.getOrDefault ^Map aliased-datatypes dtype dtype)))


(defn datatype->host-type
  "Get the signed analog of an unsigned type or return datatype unchanged."
  [datatype]
  (let [datatype (composite-datatype->base-datatype datatype)]
    (get unsigned-signed datatype datatype)))


(defn jvm-cast
  [value datatype]
  (unchecked-cast value (datatype->host-type datatype)))


(defn datatype->safe-host-type
  "Get a jvm datatype wide enough to store all values of this datatype"
  [dtype]
  (let [base-dtype (composite-datatype->base-datatype dtype)]
    (case base-dtype
        :uint8 :int16
        :uint16 :int32
        :uint32 :int64
        :uint64 :int64
        base-dtype)))

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
  (let [dtype (composite-datatype->base-datatype dtype)]
    (if (base-datatypes dtype)
      dtype
      :object)))


(defn safe-flatten
  [dtype]
  (-> dtype
      datatype->safe-host-type
      flatten-datatype))


(defn host-flatten
  [dtype]
  (-> dtype
      composite-datatype->base-datatype
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


(def buffer-access-table
  "Buffers may create readers or writers from this table alone."
  (->> base-datatypes
       (mapcat (fn [dtype]
                 (if (signed-int-types dtype)
                   [[dtype dtype]
                    [dtype (safe-flatten dtype)]]
                   [[dtype (safe-flatten dtype)]])))
       (group-by first)
       (mapcat (fn [[k v-set]]
                 (map (fn [reader-datatype]
                        {:intermediate-datatype k
                         :buffer-datatype (host-flatten k)
                         :reader-datatype reader-datatype})
                      (->> v-set
                           (map second)
                           distinct))))))


;;Everything goes to these and these go to everything.
(def base-marshal-types
  #{:int8 :int16 :int32 :int64 :float32 :float64 :boolean :object})


(defmacro make-base-datatype-table
  [inner-macro]
  `(->> [~@(for [dtype base-marshal-types]
             [dtype `(~inner-macro ~dtype)])]
        (into {})))


(defmacro make-base-no-boolean-datatype-table
  [inner-macro]
  `(->> [~@(for [dtype (->> base-marshal-types
                            (remove #(= :boolean %)))]
             [dtype `(~inner-macro ~dtype)])]
        (into {})))


(def marshal-source-table
  {:int8 base-marshal-types
   :int16 base-marshal-types
   :int32 base-marshal-types
   :int64 base-marshal-types
   :float32 base-marshal-types
   :float64 base-marshal-types
   :boolean base-marshal-types
   :object base-marshal-types})


(def marshal-table
  (->> marshal-source-table
       (mapcat (fn [[src-dtype dst-dtype-seq]]
                 (map vector (repeat src-dtype) dst-dtype-seq)))
       vec))


(defmacro make-marshalling-item-table
  [marshal-macro]
  `(->> [~@(for [[src-dtype dst-dtype] marshal-table]
             [[src-dtype dst-dtype]
              `(~marshal-macro ~src-dtype ~dst-dtype)])]
        (into {})))
