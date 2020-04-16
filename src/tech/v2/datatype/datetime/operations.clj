(ns tech.v2.datatype.datetime.operations
  (:require [tech.v2.datatype.datetime
             :refer [collapse-date-datatype]
             :as dtype-dt]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.readers.const :refer [make-const-reader]]
            [tech.v2.datatype.iterable.const :refer [make-const-iterable]]
            [tech.v2.datatype.argtypes :refer [arg->arg-type]]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.binary-op :as binary-op]
            [tech.v2.datatype.boolean-op :as boolean-op]
            [tech.v2.datatype.reduce-op :as reduce-op]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.functional :as dfn]
            [clojure.pprint :as pp]
            [primitive-math :as pmath])
  (:import [java.time ZoneId ZoneOffset
            Instant ZonedDateTime OffsetDateTime
            LocalDate LocalDateTime LocalTime
            OffsetTime Duration]
           [java.time.temporal TemporalUnit ChronoUnit
            Temporal TemporalAmount ChronoField]
           [tech.v2.datatype
            PackedInstant PackedLocalDate
            PackedLocalTime PackedLocalDateTime
            ObjectReader LongReader IntReader
            IterHelpers$LongIterConverter
            IterHelpers$IntIterConverter
            IterHelpers$ObjectIterConverter])
  (:refer-clojure :exclude [> >= < <= == min max]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn- make-temporal-chrono-plus
  [opname ^ChronoUnit chrono-unit]
  (binary-op/make-binary-op
   (keyword (format "temporal-add-%s" (name opname)))
   :object
   (when x
     (.plus ^Temporal x
            (long (Math/round
                   (if y (double y) 0.0)))
            chrono-unit))))


(defn- make-temporal-chrono-minus
  [opname ^ChronoUnit chrono-unit]
  (binary-op/make-binary-op
   (keyword (format "temporal-minus-%s" opname))
   :object
   (when x
     (.minus ^Temporal x
             (long (Math/round
                    (if y (double y) 0.0)))
             chrono-unit))))


(def ^:private temporal-numeric-ops
  (->> dtype-dt/keyword->chrono-unit
       (mapcat (fn [[k v]]
                 [[(keyword (str "plus-" (name k)))
                   (make-temporal-chrono-plus k v)]
                  [(keyword (str "minus-" (name k)))
                   (make-temporal-chrono-minus k v)]]))
       (into {} )))


(def ^:private temporal-amount-ops
  {:plus-temporal-amount
   (binary-op/make-binary-op
    :plus-temporal-amount
    :object
    (when x
      (.plus ^Temporal x ^TemporalAmount y)))
   :minus-temporal-amount
   (binary-op/make-binary-op
    :minus-temporal-amount
    :object
    (when x
      (.minus ^Temporal x ^TemporalAmount y)))})


(defn make-temporal-long-getter
  [opname ^ChronoField temporal-field]
  (unary-op/make-unary-op
   (keyword (format "temporal-get-%s" opname))
   :object
   (when x
     (.getLong ^Temporal x temporal-field))))


(def ^:private temporal-getters
  (merge (->> dtype-dt/keyword->temporal-field
               (map (fn [[k v]]
                      [k
                       (make-temporal-long-getter k v)]))
               (into {}))
          {:epoch-milliseconds
           (unary-op/make-unary-op
            (keyword "temporal-get-epoch-milliseconds")
            :object
            (when x
              (dtype-dt/->milliseconds-since-epoch x)))}))


(defn- as-chrono-unit
  ^ChronoUnit [item] item)


(defmacro ^:private make-packed-chrono-plus
  [datatype opname chrono-unit]
  (let [src-dtype (dtype-dt/packed-type->unpacked-type datatype)]
    `(binary-op/make-binary-op
      (keyword (format "%s-add-%s" (name ~datatype) (name ~opname)))
      ~datatype
      (casting/datatype->unchecked-cast-fn
       :unknown
       ~(casting/datatype->host-type datatype)
       (-> (dtype-dt/compile-time-unpack ~'x ~src-dtype)
           (.plus (long~'y) (as-chrono-unit ~chrono-unit))
           (dtype-dt/compile-time-pack ~src-dtype))))))


(defmacro ^:private make-packed-chrono-minus
  [datatype opname chrono-unit]
  (let [src-dtype (dtype-dt/packed-type->unpacked-type datatype)]
    `(binary-op/make-binary-op
      (keyword (format "%s-add-%s" (name ~datatype) (name ~opname)))
      ~datatype
      (casting/datatype->unchecked-cast-fn
       :unknown
       ~(casting/datatype->host-type datatype)
       (-> (dtype-dt/compile-time-unpack ~'x ~src-dtype)
           (.minus (long~'y) (as-chrono-unit~chrono-unit))
           (dtype-dt/compile-time-pack ~src-dtype))))))


(defmacro ^:private make-packed-numeric-ops
  [datatype]
  `(->> dtype-dt/keyword->chrono-unit
        (mapcat (fn [[k# v#]]
                  [[(keyword (str "plus-" (name k#)))
                    (make-packed-chrono-plus ~datatype k# v#)]
                   [(keyword (str "minus-" (name k#)))
                    (make-packed-chrono-minus ~datatype k# v#)]]))
        (into {})))


(defmacro ^:private make-packed-temporal-numeric-ops
  [datatype]
  (let [src-dtype (dtype-dt/packed-type->unpacked-type datatype)]
    {:plus-temporal-amount
     `(binary-op/make-binary-op
       :plus-temporal-amount
       ~datatype
       (casting/datatype->unchecked-cast-fn
        :unknown
        ~(casting/datatype->host-type datatype)
        (-> (dtype-dt/compile-time-unpack ~'x ~src-dtype)
            (.plus (dtype-dt/as-temporal-amount ~'y))
            (dtype-dt/compile-time-pack ~src-dtype))))
     :minus-temporal-amount
     `(binary-op/make-binary-op
       :minus-temporal-amount
       ~datatype
       (casting/datatype->unchecked-cast-fn
        :unknown
        ~(casting/datatype->host-type datatype)
        (-> (dtype-dt/compile-time-unpack ~'x ~src-dtype)
            (.minus (dtype-dt/as-temporal-amount ~'y))
            (dtype-dt/compile-time-pack ~src-dtype))))}))


(defmacro ^:private make-packed-temporal-getter
  [datatype opname temporal-field]
  (let [src-dtype (dtype-dt/packed-type->unpacked-type datatype)]
    `(unary-op/make-unary-op
      (keyword (format "%s-get-%s" (name ~datatype) (name ~opname)))
      :int64
      (-> (dtype-dt/compile-time-unpack ~'x ~src-dtype)
          (.getLong ~temporal-field)))))


(defmacro ^:private make-packed-getters
  [datatype]
  (let [src-dtype (dtype-dt/packed-type->unpacked-type datatype)]
    `(merge (->> dtype-dt/keyword->temporal-field
                 (map (fn [[k# v#]]
                        [k# (make-packed-temporal-getter ~datatype k# v#)]))
                 (into {}))
            {:epoch-milliseconds
             (unary-op/make-unary-op
              (keyword (format "%s-get-epoch-milliseconds" (name ~datatype)))
              :int64
              (-> (dtype-dt/compile-time-unpack ~'x ~src-dtype)
                  (dtype-dt/->milliseconds-since-epoch)))})))


(defn temporal-time-op-table
  [datatype before-fn after-fn millis-fn]
  {:numeric-ops temporal-numeric-ops
   :int64-getters temporal-getters
   :temporal-amount-ops temporal-amount-ops
   :boolean-ops
   {:< (boolean-op/make-boolean-binary-op
        :lt datatype
        (before-fn x y))
    :> (boolean-op/make-boolean-binary-op
        :gt datatype
        (after-fn x y))
    :<= (boolean-op/make-boolean-binary-op
         :lte datatype
         (or (.equals ^Object x y)
             (before-fn x y)))
    :>= (boolean-op/make-boolean-binary-op
         :gte datatype
         (or (.equals ^Object x y)
             (after-fn x y)))
    :== (boolean-op/make-boolean-binary-op
         :instant-== :instant
         (.equals ^Object x y))}
   :binary-ops
   {:min (binary-op/make-binary-op
          :min datatype
          (if (before-fn x y) x y))
    :max (binary-op/make-binary-op
          :max datatype
          (if (after-fn x y) x y))}
   :binary->int64-ops
   {:difference-milliseconds
    (binary-op/make-binary-op
     :difference-millis :object
     (- (long (millis-fn x))
        (long (millis-fn y))))}})


(defmacro packed-temporal-time-op-table
  [datatype millis-fn]
  `{:numeric-ops (make-packed-numeric-ops ~datatype)
    :int64-getters (make-packed-getters ~datatype)
    :temporal-amount-ops (make-packed-temporal-numeric-ops ~datatype)
    :boolean-ops
    {:< (boolean-op/make-boolean-binary-op
         :lt ~datatype
         (clojure.core/< ~'x ~'y))
     :> (boolean-op/make-boolean-binary-op
         :gt ~datatype
         (clojure.core/> ~'x ~'y))
     :<= (boolean-op/make-boolean-binary-op
          :lte ~datatype
          (clojure.core/<= ~'x ~'y))
     :>= (boolean-op/make-boolean-binary-op
          :gte ~datatype
          (clojure.core/>= ~'x ~'y))
     :== (boolean-op/make-boolean-binary-op
          :== ~datatype
          (clojure.core/== ~'x ~'y))}
    :binary-ops
    {:min (binary-op/make-binary-op
           :min ~datatype
           (if (clojure.core/< ~'x ~'y) ~'x ~'y))
     :max (binary-op/make-binary-op
           :max ~datatype
           (if (clojure.core/> ~'x ~'y) ~'x ~'y))}
    :binary->int64-ops
    {:difference-milliseconds
     (binary-op/make-binary-op
      :difference-millis :object
      (- (~millis-fn ~'x)
         (~millis-fn ~'y)))}})



(defn- make-duration-chrono-plus
  [opname ^ChronoUnit chrono-unit]
  (binary-op/make-binary-op
   (keyword (format "duration-add-%s" (name opname)))
   :object
   (when x
     (.plus ^Duration x
            (long (Math/round
                   (if y (double y) 0.0)))
            chrono-unit))))


(defn- make-duration-chrono-minus
  [opname ^ChronoUnit chrono-unit]
  (binary-op/make-binary-op
   (keyword (format "duration-minus-%s" opname))
   :object
   (when x
     (.minus ^Duration x
             (long (Math/round
                    (if y (double y) 0.0)))
             chrono-unit))))


(def ^:private duration-numeric-ops
  (->> dtype-dt/keyword->chrono-unit
       (mapcat (fn [[k v]]
                 [[(keyword (str "plus-" (name k)))
                   (make-duration-chrono-plus k v)]
                  [(keyword (str "minus-" (name k)))
                   (make-duration-chrono-minus k v)]]))
       (into
        {:duration-plus-duration
         (binary-op/make-binary-op
          :duration-add-duration
          :object
          (when x
            (.plus ^Duration x ^Duration y)))
         :duration-minus-duration
         (binary-op/make-binary-op
          :duration-minus-duration
          :object
          (when x
            (.minus ^Duration x ^Duration y)))})))


(defn make-duration-long-getter
  [opname ^ChronoUnit temporal-unit]
  (unary-op/make-unary-op
   (keyword (format "duration-get-%s" opname))
   :object
   (when x
     (.get ^Duration x temporal-unit))))


(def ^:private duration-getters
  {:nanoseconds
   (unary-op/make-unary-op
    :duration-get-nanoseconds
    :object
    (when x
      (dtype-dt/duration->nanoseconds x)))
   :milliseconds
   (unary-op/make-unary-op
    :duration-get-milliseconds
    :object
    (when x
      (dtype-dt/duration->milliseconds x)))
   :seconds
   (unary-op/make-unary-op
    :duration-get-seconds
    :object
    (when x
      (.getSeconds ^Duration x)))
   :minutes
   (unary-op/make-unary-op
    :duration-get-minutes
    :object
    (when x
      (.toMinutes ^Duration x)))
   :hours
   (unary-op/make-unary-op
    :duration-get-hours
    :object
    (when x
      (.toHours ^Duration x)))
   :days
   (unary-op/make-unary-op
    :duration-get-hours
    :object
    (when x
      (.toDays ^Duration x)))})


(defn- duration-before
  [lhs rhs]
  (clojure.core/< (.compareTo ^Duration lhs ^Duration rhs) 0))


(defn- duration-after
  [lhs rhs]
  (clojure.core/> (.compareTo ^Duration lhs ^Duration rhs) 0))


(def ^:private duration-time-ops
   {:numeric-ops duration-numeric-ops
    :int64-getters duration-getters
    :boolean-ops
    {:< (boolean-op/make-boolean-binary-op
        :lt :duration
        (duration-before x y))
     :> (boolean-op/make-boolean-binary-op
         :gt :duration
         (duration-after x y))
     :<= (boolean-op/make-boolean-binary-op
          :lte :duration
          (or (.equals ^Object x y)
              (duration-before x y)))
     :>= (boolean-op/make-boolean-binary-op
          :gte :duration
          (or (.equals ^Object x y)
              (duration-after x y)))
     :== (boolean-op/make-boolean-binary-op
          :instant-== :instant
          (.equals ^Object x y))}
    :binary-ops
   {:min (binary-op/make-binary-op
          :min :duration
          (if (duration-before x y) x y))
    :max (binary-op/make-binary-op
          :max :duration
          (if (duration-after x y) x y))}
    :binary->int64-ops
   {:difference-milliseconds
    (binary-op/make-binary-op
     :duration-difference-millis :object
     (- (long (dtype-dt/duration->milliseconds x))
        (long (dtype-dt/duration->milliseconds y))))
    :difference-nanoseconds
    (binary-op/make-binary-op
     :duration-difference-nanos :object
     (- (long (dtype-dt/duration->milliseconds x))
        (long (dtype-dt/duration->milliseconds y))))
    :difference-duration
    (binary-op/make-binary-op
     :duration-difference-duration :object
     (.minus ^Duration x ^Duration y))}})

(def packed-duration-conversions
  [:nanoseconds :milliseconds
   :seconds :minutes :hours
   :days :weeks])


(defmacro ^:private packed-duration-conversion-table
  [datatype]
  (case datatype
    :nanoseconds 1
    :milliseconds `(dtype-dt/nanoseconds-in-millisecond)
    :seconds `(dtype-dt/nanoseconds-in-second)
    :minutes `(dtype-dt/nanoseconds-in-minute)
    :hours `(dtype-dt/nanoseconds-in-hour)
    :days `(dtype-dt/nanoseconds-in-day)
    :weeks `(dtype-dt/nanoseconds-in-week)))


(defmacro ^:private packed-duration-numeric-ops
  []
  (->> packed-duration-conversions
       (mapcat (fn [conv-name]
                 (let [plus-name (keyword (str "plus-" (name conv-name)))
                       minus-name (keyword (str "minus-" (name conv-name)))]
                   [[plus-name
                     `(binary-op/make-binary-op
                       ~plus-name :int64
                       (pmath/+ ~'x (pmath/* ~'y (packed-duration-conversion-table
                                                  ~conv-name))))]
                    [minus-name
                     `(binary-op/make-binary-op
                       ~minus-name :int64
                       (pmath/+ ~'x (pmath/* ~'y (packed-duration-conversion-table
                                                  ~conv-name))))]])))
       (into {})))


(defmacro ^:private packed-duration-getters
  []
  (->> packed-duration-conversions
       (mapcat (fn [conv-name]
                 [[conv-name
                   `(unary-op/make-unary-op
                     ~conv-name :int64
                     (pmath/+ (quot ~'x
                                    (packed-duration-conversion-table
                                     ~conv-name)
                                    )))]]))
       (into {})))


(def packed-duration-time-ops
  {:numeric-ops (packed-duration-numeric-ops)
   :int64-getters (packed-duration-getters)
   :boolean-ops
    {:< (boolean-op/make-boolean-binary-op
        :lt :packed-duration
        (pmath/< x y))
     :> (boolean-op/make-boolean-binary-op
         :gt :packed-duration
         (pmath/> x y))
     :<= (boolean-op/make-boolean-binary-op
          :lte :packed-duration
          (or (pmath/== x y)
              (pmath/< x y)))
     :>= (boolean-op/make-boolean-binary-op
          :gte :packed-duration
          (or (pmath/== x y)
              (pmath/> x y)))
     :== (boolean-op/make-boolean-binary-op
          :instant-== :packed-duration
          (pmath/== x y))}
    :binary-ops
   {:min (binary-op/make-binary-op
          :min :packed-duration
          (pmath/min x y))
    :max (binary-op/make-binary-op
          :max :packed-duration
          (pmath/max x y))}
    :binary->int64-ops
   {:difference-milliseconds
    (binary-op/make-binary-op
     :packed-duration-difference-millis :packed-duration
     (quot (pmath/- x y)
           (dtype-dt/nanoseconds-in-millisecond)))
    :difference-nanoseconds
    (binary-op/make-binary-op
     :packed-duration-difference-nanos :packed-duration
     (pmath/- x y))}})


(def java-time-ops
  {:duration duration-time-ops
   :packed-duration packed-duration-time-ops
   :instant
   (temporal-time-op-table :instant
                           #(.isBefore ^Instant %1 ^Instant %2)
                           #(.isAfter ^Instant %1 ^Instant %2)
                           dtype-dt/instant->milliseconds-since-epoch)
   :packed-instant
   (packed-temporal-time-op-table
    :packed-instant
    dtype-dt/packed-instant->milliseconds-since-epoch)

   :zoned-date-time
   (temporal-time-op-table :zoned-date-time
                           #(.isBefore ^ZonedDateTime %1 ^ZonedDateTime %2)
                           #(.isAfter ^ZonedDateTime %1 ^ZonedDateTime %2)
                           dtype-dt/zoned-date-time->milliseconds-since-epoch)

   :offset-date-time
   (temporal-time-op-table :offset-date-time
                           #(.isBefore ^OffsetDateTime %1 ^OffsetDateTime %2)
                           #(.isAfter ^OffsetDateTime %1 ^OffsetDateTime %2)
                           dtype-dt/offset-date-time->milliseconds-since-epoch)

   :local-date-time
   (temporal-time-op-table :local-date-time
                           #(.isBefore ^LocalDateTime %1 ^LocalDateTime %2)
                           #(.isAfter ^LocalDateTime %1 ^LocalDateTime %2)
                           dtype-dt/local-date-time->milliseconds-since-epoch)
   :packed-local-date-time
   (packed-temporal-time-op-table
    :packed-local-date-time
    dtype-dt/packed-local-date-time->milliseconds-since-epoch)


   :local-date
   (temporal-time-op-table :local-date
                           #(.isBefore ^LocalDate %1 ^LocalDate %2)
                           #(.isAfter ^LocalDate %1 ^LocalDate %2)
                           dtype-dt/local-date->milliseconds-since-epoch)
   :packed-local-date
   (packed-temporal-time-op-table
    :packed-local-date
    dtype-dt/packed-local-date->milliseconds-since-epoch)

   :local-time
   (temporal-time-op-table :local-time
                           #(.isBefore ^LocalTime %1 ^LocalTime %2)
                           #(.isAfter ^LocalTime %1 ^LocalTime %2)
                           dtype-dt/local-time->milliseconds)

   :packed-local-time
   (packed-temporal-time-op-table
    :packed-local-time
    dtype-dt/packed-local-time->milliseconds)
})


(defn- argtypes->operation-type
  [lhs-argtype rhs-argtype]
  (cond
    (and (= lhs-argtype :scalar)
         (= rhs-argtype :scalar))
    :scalar
    (or (= lhs-argtype :iterable)
        (= rhs-argtype :iterable))
    :iterable
    :else
    :reader))


(defn- promote-op-arg
  "Promote a scalar argument so it is the type of the argtypes"
  [op-argtype arg-argtype arg other-arg]
  (if (= arg-argtype :scalar)
    (let [arg-dtype (dtype-base/get-datatype arg)]
      (case op-argtype
        :iterable (make-const-iterable arg arg-dtype)
        :reader (make-const-reader arg arg-dtype
                                   (dtype-base/ecount other-arg))))
    arg))


(defn- make-iterable-of-type
  [iterable datatype]
  (if-not (= datatype (dtype-proto/get-datatype iterable))
    (dtype-proto/->iterable iterable {:datatype datatype})
    iterable))


(defn- make-reader-of-type
  [reader datatype]
  (if-not (= datatype (dtype-proto/get-datatype reader))
    (dtype-proto/->reader reader {:datatype datatype})
    reader))


(defn- perform-int64-getter
  [lhs unary-op-name]
  (let [lhs-argtype (arg->arg-type lhs)
        lhs-dtype (collapse-date-datatype lhs)
        lhs (if (dtype-dt/packed-datatype? lhs-dtype)
              (dtype-proto/set-datatype lhs :int64)
              lhs)
        unary-op (get-in java-time-ops [lhs-dtype :int64-getters
                                        unary-op-name])
        result-dtype (if (or (= unary-op-name :epoch-milliseconds)
                             (= unary-op-name :epoch-seconds))
                       unary-op-name
                       :int64)]
    (when-not unary-op
      (throw (Exception. (format "Could not find getter: %s" unary-op-name) )))
    (->
     (case lhs-argtype
       :scalar
       (unary-op lhs)
       :iterable
       (unary-op/unary-iterable-map {} unary-op lhs)
       :reader
       (unary-op/unary-reader-map {} unary-op lhs))
     (dtype-proto/set-datatype result-dtype))))


(defn- perform-time-op
  [lhs rhs op-dtype bin-op]
  (let [lhs-argtype (arg->arg-type lhs)
        rhs-argtype (arg->arg-type rhs)
        op-argtype (argtypes->operation-type lhs-argtype rhs-argtype)]
    (case op-argtype
      :scalar
      (bin-op lhs rhs)
      :iterable
      (binary-op/binary-iterable-map
       {:datatype op-dtype}
       bin-op
       (promote-op-arg op-argtype lhs-argtype lhs rhs)
       (promote-op-arg op-argtype rhs-argtype rhs lhs))
      (binary-op/binary-reader-map
       {:datatype op-dtype}
       bin-op
       (promote-op-arg op-argtype lhs-argtype lhs rhs)
       (promote-op-arg op-argtype rhs-argtype rhs lhs)))))

(defn- perform-commutative-numeric-op
  [lhs rhs opname]
  (let [lhs-dtype (collapse-date-datatype lhs)
        rhs-dtype (collapse-date-datatype rhs)
        any-number? (or (= :number lhs-dtype)
                        (= :number rhs-dtype))
        any-date-datatype? (or (dtype-dt/datetime-datatype? lhs-dtype)
                               (dtype-dt/datetime-datatype? rhs-dtype))
        _ (when-not any-number?
            (throw (Exception. (format "Arguments must have numeric type: %s, %s"
                                       lhs-dtype rhs-dtype))))
        _ (when-not any-date-datatype?
            (throw (Exception. (format "Arguments not datetime related: %s, %s"
                                       lhs-dtype rhs-dtype))))
        ;;There is an assumption that the arguments are commutative and the left
        ;;hand side is the actual arg.
        lhs-num? (= :number lhs-dtype)
        numeric-arg (if lhs-num? lhs rhs)
        date-time-arg (if lhs-num? rhs lhs)
        date-time-dtype (if lhs-num? rhs-dtype lhs-dtype)
        num-op (get-in java-time-ops [date-time-dtype :numeric-ops opname])]
    (when-not num-op
      (throw (Exception. (format "Could not find numeric op %s for type %s"
                                 opname date-time-dtype))))
    (perform-time-op date-time-arg numeric-arg date-time-dtype num-op)))


(defn- perform-non-commutative-numeric-op
  "only the left side can be a datatype"
  [lhs rhs opname]
  (let [lhs-dtype (collapse-date-datatype lhs)
        rhs-dtype (collapse-date-datatype rhs)
        any-number? (= :number rhs-dtype)
        any-date-datatype? (dtype-dt/datetime-datatype? lhs-dtype)
        _ (when-not any-number?
            (throw (Exception. (format "Arguments must have numeric type: %s, %s"
                                       lhs-dtype rhs-dtype))))
        _ (when-not any-date-datatype?
            (throw (Exception. (format "Arguments not datetime related: %s, %s"
                                       lhs-dtype rhs-dtype))))
        ;;There is an assumption that the arguments are commutative and the left
        ;;hand side is the actual arg.
        numeric-arg rhs
        date-time-arg lhs
        date-time-dtype lhs-dtype
        num-op (get-in java-time-ops [date-time-dtype :numeric-ops opname])]
    (when-not num-op
      (throw (Exception. (format "Could not find numeric op %s for type %s"
                                 opname date-time-dtype))))
    (perform-time-op date-time-arg numeric-arg date-time-dtype num-op)))


(defn temporal-amount-datatype?
  [dtype]
  (or
   (= dtype :duration)
   (= dtype :packed-duration)))


(defn- perform-commutative-temporal-amount-op
  [lhs rhs opname]
  (let [lhs-dtype (collapse-date-datatype lhs)
        rhs-dtype (collapse-date-datatype rhs)
        any-number? (or (temporal-amount-datatype? lhs-dtype)
                        (temporal-amount-datatype? rhs-dtype))
        any-date-datatype? (or (dtype-dt/datetime-datatype? lhs-dtype)
                               (dtype-dt/datetime-datatype? rhs-dtype))
        _ (when-not any-number?
            (throw (Exception. (format
                                "Arguments must have temporal amount type: %s, %s"
                                lhs-dtype rhs-dtype))))
        _ (when-not any-date-datatype?
            (throw (Exception. (format "Arguments not datetime related: %s, %s"
                                       lhs-dtype rhs-dtype))))
        ;;There is an assumption that the arguments are commutative and the left
        ;;hand side is the actual arg.
        lhs-num? (temporal-amount-datatype? lhs-dtype)
        numeric-arg (if lhs-num? lhs rhs)
        numeric-arg (if (dtype-dt/packed-datatype?
                         (dtype-base/get-datatype numeric-arg))
                      (dtype-dt/unpack numeric-arg)
                      numeric-arg)
        date-time-arg (if lhs-num? rhs lhs)
        date-time-dtype (if lhs-num? rhs-dtype lhs-dtype)
        num-op (get-in java-time-ops [date-time-dtype :temporal-amount-ops opname])]
    (when-not num-op
      (throw (Exception. (format "Could not find numeric op %s for type %s"
                                 opname date-time-dtype))))
    (perform-time-op date-time-arg numeric-arg date-time-dtype num-op)))


(defn- perform-non-commutative-temporal-amount-op
  "only the left side can be a datatype"
  [lhs rhs opname]
  (let [lhs-dtype (collapse-date-datatype lhs)
        rhs-dtype (collapse-date-datatype rhs)
        any-number? (temporal-amount-datatype? rhs-dtype)
        any-date-datatype? (dtype-dt/datetime-datatype? lhs-dtype)
        _ (when-not any-number?
            (throw (Exception. (format
                                "Arguments must have temporal amount type: %s, %s"
                                lhs-dtype rhs-dtype))))
        _ (when-not any-date-datatype?
            (throw (Exception. (format "Arguments not datetime related: %s, %s"
                                       lhs-dtype rhs-dtype))))
        ;;There is an assumption that the arguments are commutative and the left
        ;;hand side is the actual arg.
        numeric-arg rhs
        numeric-arg (if (dtype-dt/packed-datatype?
                         (dtype-base/get-datatype numeric-arg))
                      (dtype-dt/unpack numeric-arg)
                      numeric-arg)
        date-time-arg lhs
        date-time-dtype lhs-dtype
        num-op (get-in java-time-ops [date-time-dtype :temporal-amount-ops opname])]
    (when-not num-op
      (throw (Exception. (format "Could not find numeric op %s for type %s"
                                 opname date-time-dtype))))
    (perform-time-op date-time-arg numeric-arg date-time-dtype num-op)))


(defn- perform-duration-numeric-op ;;either plus or minus
  [lhs rhs opname]
  (let [lhs-dtype (collapse-date-datatype lhs)
        rhs-dtype (collapse-date-datatype rhs)
        _ (when-not (and (temporal-amount-datatype? lhs-dtype)
                         (temporal-amount-datatype? rhs-dtype))
            (throw (Exception. (format
                                "Arguments must have duration type: %s, %s"
                                lhs-dtype rhs-dtype))))
        num-op (get-in java-time-ops [lhs-dtype :numeric-ops opname])]
    (when-not num-op
      (throw (Exception. (format "Could not find numeric op %s for type %s"
                                 opname lhs-dtype))))
    (perform-time-op lhs rhs lhs-dtype num-op)))


(defn- perform-boolean-op
  [lhs rhs opname]
  (let [lhs-dtype (collapse-date-datatype lhs)
        rhs-dtype (collapse-date-datatype rhs)
        any-date-datatype? (or (dtype-dt/datetime-datatype? lhs-dtype)
                               (dtype-dt/datetime-datatype? rhs-dtype))
        _ (when-not any-date-datatype?
            (throw (Exception. (format "Arguments not datetime related: %s, %s"
                                       lhs-dtype rhs-dtype))))
        _ (when-not (= lhs-dtype rhs-dtype)
            (throw (Exception. (format "Argument types do not match: %s, %s"
                                       lhs-dtype rhs-dtype))))
        bool-op (get-in java-time-ops [lhs-dtype :boolean-ops opname])]
    (when-not bool-op
      (throw (Exception. (format "Could not find boolean op %s for type %s"
                                 opname lhs-dtype))))
    (let [lhs-argtype (arg->arg-type lhs)
          rhs-argtype (arg->arg-type rhs)
          op-argtype (argtypes->operation-type lhs-argtype rhs-argtype)]
      (case op-argtype
        :scalar
        (bool-op lhs rhs)
        :iterable
        (boolean-op/boolean-binary-iterable-map
         {}
         bool-op
         (promote-op-arg op-argtype lhs-argtype lhs rhs)
         (promote-op-arg op-argtype rhs-argtype rhs lhs))
        (boolean-op/boolean-binary-reader-map
         {}
         bool-op
         (promote-op-arg op-argtype lhs-argtype lhs rhs)
         (promote-op-arg op-argtype rhs-argtype rhs lhs))))))


(defn- perform-binary-op
  [lhs rhs opname]
  (let [lhs-dtype (collapse-date-datatype lhs)
        rhs-dtype (collapse-date-datatype rhs)
        any-date-datatype? (or (dtype-dt/datetime-datatype? lhs-dtype)
                               (dtype-dt/datetime-datatype? rhs-dtype))
        _ (when-not any-date-datatype?
            (throw (Exception. (format "Arguments not datetime related: %s, %s"
                                       lhs-dtype rhs-dtype))))
        _ (when-not (= lhs-dtype rhs-dtype)
            (throw (Exception. (format "Argument types do not match: %s, %s"
                                       lhs-dtype rhs-dtype))))
        binary-op (get-in java-time-ops [lhs-dtype :binary-ops opname])]
    (when-not binary-op
      (throw (Exception. (format "Could not find binary op %s for type %s"
                                 opname lhs-dtype))))
    (let [lhs-argtype (arg->arg-type lhs)
          rhs-argtype (arg->arg-type rhs)
          op-argtype (argtypes->operation-type lhs-argtype rhs-argtype)]
      (case op-argtype
        :scalar
        (binary-op lhs rhs)
        :iterable
        (binary-op/binary-iterable-map
         {:datatype lhs-dtype}
         binary-op
         (promote-op-arg op-argtype lhs-argtype lhs rhs)
         (promote-op-arg op-argtype rhs-argtype rhs lhs))
        (binary-op/binary-reader-map
         {:datatype lhs-dtype}
         binary-op
         (promote-op-arg op-argtype lhs-argtype lhs rhs)
         (promote-op-arg op-argtype rhs-argtype rhs lhs))))))


(defn- perform-commutative-binary-reduction
  [lhs opname]
  (let [lhs-dtype (collapse-date-datatype lhs)
        any-date-datatype? (dtype-dt/datetime-datatype? lhs-dtype)
        _ (when-not any-date-datatype?
            (throw (Exception. (format "Argument not datetime related: %s"
                                       lhs-dtype))))
        binary-op (get-in java-time-ops [lhs-dtype :binary-ops opname])]
    (when-not binary-op
      (throw (Exception. (format "Could not find binary op %s for type %s"
                                 opname lhs-dtype))))
    (if (= :scalar (arg->arg-type lhs))
      lhs
      (reduce-op/commutative-reader-reduce {} binary-op lhs))))


(defn- perform-binary->int64-op
  [lhs rhs opname]
  (let [lhs-dtype (collapse-date-datatype lhs)
        rhs-dtype (collapse-date-datatype rhs)
        any-date-datatype? (or (dtype-dt/datetime-datatype? lhs-dtype)
                               (dtype-dt/datetime-datatype? rhs-dtype))
        _ (when-not any-date-datatype?
            (throw (Exception. (format "Arguments not datetime related: %s, %s"
                                       lhs-dtype rhs-dtype))))
        _ (when-not (= lhs-dtype rhs-dtype)
            (throw (Exception. (format "Argument types do not match: %s, %s"
                                       lhs-dtype rhs-dtype))))
        binary-op (get-in java-time-ops [lhs-dtype :binary->int64-ops opname])]
    (when-not binary-op
      (throw (Exception. (format "Could not find binary op %s for type %s"
                                 opname lhs-dtype))))
    (let [lhs-argtype (arg->arg-type lhs)
          rhs-argtype (arg->arg-type rhs)
          op-argtype (argtypes->operation-type lhs-argtype rhs-argtype)]
      (case op-argtype
        :scalar
        (binary-op lhs rhs)
        :iterable
        (-> (binary-op/binary-iterable-map
             {}
             binary-op
             (promote-op-arg op-argtype lhs-argtype lhs rhs)
             (promote-op-arg op-argtype rhs-argtype rhs lhs))
            (dtype-proto/->iterable {:datatype :int64}))
        (-> (binary-op/binary-reader-map
             {}
             binary-op
             (promote-op-arg op-argtype lhs-argtype lhs rhs)
             (promote-op-arg op-argtype rhs-argtype rhs lhs))
            (dtype-proto/->iterable {:datatype :int64}))))))


(defmacro ^:private declare-int64-getters
  []
  `(do
     ~@(->> dtype-dt/keyword->temporal-field
            (map (fn [[k v]]
                   `(defn ~(symbol (str "get-" (name k)))
                      [~'item]
                      (perform-int64-getter ~'item ~k)))))))


(declare-int64-getters)


(defn get-milliseconds
  [item]
  (perform-int64-getter item :milliseconds))


(defn get-nanoseconds
  [item]
  (perform-int64-getter item :nanoseconds))


(defn get-epoch-milliseconds
  [item]
  (perform-int64-getter item :epoch-milliseconds))


(defn get-epoch-seconds
  [item]
  (-> (dfn/quot
       (perform-int64-getter item :epoch-milliseconds)
       (dtype-dt/milliseconds-in-second))
      (dtype-proto/set-datatype :epoch-seconds)))


(defn get-epoch-minutes
  [item]
  (dfn/quot (get-epoch-milliseconds item)
            (dtype-dt/milliseconds-in-minute)))


(defn get-epoch-hours
  [item]
  (dfn/quot (get-epoch-milliseconds item)
            (dtype-dt/milliseconds-in-hour)))


(defn get-epoch-days
  [item]
  (dfn/quot (get-epoch-milliseconds item)
            (dtype-dt/milliseconds-in-day)))


(defn get-epoch-weeks
  [item]
  (dfn/quot (get-epoch-milliseconds item)
            (dtype-dt/milliseconds-in-week)))


(defmacro ^:private declare-plus-minus-ops
  []
  `(do
     ~@(->> dtype-dt/keyword->chrono-unit
            (mapcat (fn [[k v]]
                      [`(defn ~(symbol (str "plus-" (name k)))
                          [~'lhs ~'rhs]
                          (perform-commutative-numeric-op
                           ~'lhs ~'rhs ~(keyword (str "plus-" (name k)))))
                       `(defn ~(symbol (str "minus-" (name k)))
                          [~'lhs ~'rhs]
                          (perform-non-commutative-numeric-op
                           ~'lhs ~'rhs ~(keyword (str "minus-" (name k)))))])))))


(declare-plus-minus-ops)


(defn plus-temporal-amount
  [lhs rhs]
  (perform-commutative-temporal-amount-op
   lhs rhs :plus-temporal-amount))


(defn minus-temporal-amount
  [lhs rhs]
  (perform-non-commutative-temporal-amount-op
   lhs rhs :minus-temporal-amount))


(defn plus-duration
  [lhs rhs]
  (cond
    (= :duration (dtype-base/get-datatype lhs))
    (perform-duration-numeric-op lhs rhs :duration-plus-duration)
    (= :packed-duration (dtype-base/get-datatype lhs))
    (if (= :packed-duration (dtype-base/get-datatype rhs))
      (perform-duration-numeric-op lhs rhs :plus-nanoseconds)
      (dtype-dt/pack (plus-duration (dtype-dt/unpack lhs)) rhs))
    (= :packed-duration dtype-base/get-datatype rhs)
    (plus-temporal-amount lhs (dtype-dt/unpack rhs))
    :else
    (plus-temporal-amount lhs rhs)))


(defn minus-duration
  [lhs rhs]
  (cond
    (= :duration (dtype-base/get-datatype lhs))
    (perform-duration-numeric-op lhs rhs :duration-minus-duration)
    (= :packed-duration (dtype-base/get-datatype lhs))
    (if (= :packed-duration (dtype-base/get-datatype rhs))
      (perform-duration-numeric-op lhs rhs :minus-nanoseconds)
      (dtype-dt/pack (plus-duration (dtype-dt/unpack lhs)) rhs))
    (= :packed-duration dtype-base/get-datatype rhs)
    (minus-temporal-amount lhs (dtype-dt/unpack rhs))
    :else
    (minus-temporal-amount lhs rhs)))


(defn <
  [lhs rhs]
  (perform-boolean-op lhs rhs :<))

(defn <=
  [lhs rhs]
  (perform-boolean-op lhs rhs :<=))

(defn >
  [lhs rhs]
  (perform-boolean-op lhs rhs :>))

(defn >=
  [lhs rhs]
  (perform-boolean-op lhs rhs :>=))

(defn ==
  [lhs rhs]
  (perform-boolean-op lhs rhs :==))

(defn min
  [lhs rhs]
  (perform-binary-op lhs rhs :min))

(defn max
  [lhs rhs]
  (perform-binary-op lhs rhs :max))

(defn reduce-min
  [lhs]
  (perform-commutative-binary-reduction lhs :min))

(defn reduce-max
  [lhs]
  (perform-commutative-binary-reduction lhs :max))

(defn difference-milliseconds
  [lhs rhs]
  (perform-binary->int64-op lhs rhs :difference-milliseconds))

(defn difference-seconds
  [lhs rhs]
  (dfn/quot (difference-milliseconds lhs rhs)
            (dtype-dt/milliseconds-in-second)))

(defn difference-minutes
  [lhs rhs]
  (dfn/quot (difference-milliseconds lhs rhs)
            (dtype-dt/milliseconds-in-minute)))

(defn difference-hours
  [lhs rhs]
  (dfn/quot (difference-milliseconds lhs rhs)
            (dtype-dt/milliseconds-in-hour)))

(defn difference-days
  [lhs rhs]
  (dfn/quot (difference-milliseconds lhs rhs)
            (dtype-dt/milliseconds-in-day)))

(defn difference-weeks
  [lhs rhs]
  (dfn/quot (difference-milliseconds lhs rhs)
            (dtype-dt/milliseconds-in-week)))


(defn millisecond-descriptive-stats
  "Get the descriptive stats.  Stats are calulated in milliseconds and
  then min, mean, max are returned as objects of the unpacked datetime
  datatype.  Any other stats values are returned in milliseconds unless
  the input is a duration or packed duration type in which case standard
  deviation is also a duration datatype."
  ([data stats-seq]
   (let [stats-set (set stats-seq)
         datatype (dtype-base/get-datatype data)
         _ (when-not (dtype-dt/datetime-datatype? datatype)
             (throw (Exception. (format "%s is not a datetime datatype"
                                        datatype))))
         numeric-data (if (dtype-dt/millis-datatypes datatype)
                        (get-milliseconds data)
                        (get-epoch-milliseconds data))
         unpacked-datatype (get dtype-dt/packed-type->unpacked-type-table
                                datatype datatype)
         value-stats (if (dtype-dt/duration-datatype? datatype)
                       #{:min :max :mean :standard-deviation}
                       #{:min :max :mean})
         stats-data (dfn/descriptive-stats numeric-data stats-set)]
     (->> stats-data
          (map (fn [[k v]]
                 [k
                  (if (value-stats k)
                    (dtype-dt/from-milliseconds v unpacked-datatype)
                    v)]))
          (into {}))))
  ([data]
   (if (dtype-dt/duration-datatype? (dtype-base/get-datatype data))
     (millisecond-descriptive-stats data #{:min :mean :max :standard-deviation})
     (millisecond-descriptive-stats data #{:min :mean :max}))))


(defn field-compatibility-matrix
  []
  (let [fieldnames (->> dtype-dt/keyword->temporal-field
                        (map first)
                        (concat [:epoch-milliseconds]))]
    (for [[datatype ctor] (sort-by first dtype-dt/datatype->constructor-fn)]
      (->>
       (for [field-name fieldnames]
         [field-name (try
                     (let [accessor (get-in java-time-ops
                                            [datatype :int64-getters field-name])]
                       (accessor (ctor))
                       true)
                     (catch Throwable e false))])
       (into {})
       (merge {:datatype datatype})))))


(defn print-compatibility-matrix
  ([m]
   (let [field-names (->> (keys (first m))
                          (remove #(= :datatype %))
                          sort)]
     (pp/print-table (concat [:datatype] field-names) m))
   nil))

(defn print-field-compatibility-matrix
  []
  (print-compatibility-matrix
   (field-compatibility-matrix)))


(defn plus-op-compatibility-matrix
  []
  (let [plus-ops (->> dtype-dt/keyword->chrono-unit
                      (map (comp #(keyword (str "plus-" (name %))) first))
                      sort)
        datatypes (sort-by first dtype-dt/datatype->constructor-fn)]
    (for [[datatype ctor] datatypes]
      (->>
       (for [plus-op plus-ops]
         [plus-op (try
                    (let [plus-op (get-in
                                   java-time-ops
                                   [datatype :numeric-ops plus-op])]
                      (plus-op (ctor) 1)
                      true)
                    (catch Throwable e false))])
       (into {})
       (merge {:datatype datatype})))))


(defn print-plus-op-compatibility-matrix
  []
  (print-compatibility-matrix
   (plus-op-compatibility-matrix)))
