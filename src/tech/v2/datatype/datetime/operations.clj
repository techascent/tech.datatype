(ns tech.v2.datatype.datetime.operations
  (:require [tech.v2.datatype.datetime
             :refer [collapse-date-datatype]
             :as dtype-dt]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.readers.const :refer [make-const-reader]]
            [tech.v2.datatype.iterable.const :refer [make-const-iterable]]
            [tech.v2.datatype.iterator :as dtype-iter]
            [tech.v2.datatype.readers.const :as const-rdr]
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
  `{:boolean-ops
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
           (if (clojure.core/> ~'x ~'y) ~'x ~'y))}})



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
      (/ (dtype-dt/duration->milliseconds x)
         1000.0)))
   :minutes
   (unary-op/make-unary-op
    :duration-get-minutes
    :object
    (when x
      (/ (dtype-dt/duration->milliseconds x)
         60000.0)))
   :hours
   (unary-op/make-unary-op
    :duration-get-hours
    :object
    (when x
      (/ (dtype-dt/duration->milliseconds x)
         3600000.0)))
   :days
   (unary-op/make-unary-op
    :duration-get-hours
    :object
    (when x
      (/ (dtype-dt/duration->milliseconds x)
         86400000.0)))
   :weeks
   (unary-op/make-unary-op
    :duration-get-weeks
    :object
    (when x
      (/ (dtype-dt/duration->milliseconds x)
         6.048E8)))})


(defn- duration-before
  [lhs rhs]
  (clojure.core/< (.compareTo ^Duration lhs ^Duration rhs) 0))


(defn- duration-after
  [lhs rhs]
  (clojure.core/> (.compareTo ^Duration lhs ^Duration rhs) 0))


(def ^:private duration-time-ops
   {:numeric-ops duration-numeric-ops
    :duration-getters duration-getters
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


(def packed-duration-time-ops
  {:boolean-ops
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
          (pmath/max x y))}})


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
    dtype-dt/packed-local-time->milliseconds)})


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


(defn- perform-int64-getter
  [lhs unary-op-name]
  (let [lhs (dtype-dt/unpack lhs)
        lhs-argtype (arg->arg-type lhs)
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


(defn- perform-duration-getter
  [lhs unary-op-name]
  (let [lhs (dtype-dt/unpack lhs)
        lhs-argtype (arg->arg-type lhs)
        lhs-dtype (collapse-date-datatype lhs)
        op-dtype (if (#{:nanoseconds :milliseconds} unary-op-name)
                   :int64
                   :float64)
        unary-op (get-in java-time-ops [lhs-dtype :duration-getters
                                        unary-op-name])]
    (when-not unary-op
      (throw (Exception. (format "Could not find getter: %s" unary-op-name) )))
    (-> (case lhs-argtype
          :scalar
          (unary-op lhs)
          :iterable
          (unary-op/unary-iterable-map {} unary-op lhs)
          :reader
          (unary-op/unary-reader-map {} unary-op lhs))
        (dtype-proto/set-datatype op-dtype))))


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


(defn- with-packing-time-op
  [date-time-arg rhs op-category opname]
  (let [date-time-orig-dtype (collapse-date-datatype date-time-arg)
        date-time-arg (dtype-dt/unpack date-time-arg)
        date-time-dtype (collapse-date-datatype date-time-arg)
        num-op (get-in java-time-ops [date-time-dtype op-category opname])
        _ (when-not num-op
            (throw (Exception. (format "Could not find numeric op %s for type %s"
                                       opname date-time-dtype))))
        result (perform-time-op date-time-arg rhs date-time-dtype num-op)]
    (if (dtype-dt/packed-datatype? date-time-orig-dtype)
      (dtype-dt/pack result)
      result)))


(defn- perform-commutative-numeric-op
  [lhs rhs opname]
  (let [lhs-dtype (collapse-date-datatype lhs)
        rhs-dtype (collapse-date-datatype rhs)
        any-number? (or (= :number lhs-dtype)
                        (= :number rhs-dtype))
        any-date-datatype? (or (dtype-dt/datetime-datatype? lhs-dtype)
                               (dtype-dt/datetime-datatype? rhs-dtype))
        _ (when-not any-number?
            (throw (Exception. (format "One Argument must have numeric type: %s, %s"
                                       lhs-dtype rhs-dtype))))
        _ (when-not any-date-datatype?
            (throw (Exception. (format "One Argument must be datetime related: %s, %s"
                                       lhs-dtype rhs-dtype))))
        ;;There is an assumption that the arguments are commutative and the left
        ;;hand side is the actual arg.
        lhs-num? (= :number lhs-dtype)
        numeric-arg (if lhs-num? lhs rhs)
        date-time-arg (if lhs-num? rhs lhs)]
    (with-packing-time-op date-time-arg numeric-arg :numeric-ops opname)))


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
                                       lhs-dtype rhs-dtype))))]
    (with-packing-time-op lhs rhs :numeric-ops opname)))


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
        numeric-arg (dtype-dt/unpack (if lhs-num? lhs rhs))
        date-time-arg (if lhs-num? rhs lhs)]
    (with-packing-time-op date-time-arg numeric-arg :temporal-amount-ops opname)))


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
                                       lhs-dtype rhs-dtype))))]
    (with-packing-time-op lhs (dtype-dt/unpack rhs) :temporal-amount-ops opname)))


(defn- perform-duration-numeric-op ;;either plus or minus
  [lhs rhs opname]
  (let [lhs-dtype (collapse-date-datatype lhs)
        rhs-dtype (collapse-date-datatype rhs)
        _ (when-not (and (temporal-amount-datatype? lhs-dtype)
                         (temporal-amount-datatype? rhs-dtype))
            (throw (Exception. (format
                                "Arguments must have duration type: %s, %s"
                                lhs-dtype rhs-dtype))))]
    (with-packing-time-op lhs (dtype-dt/unpack rhs) :numeric-ops opname)))


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
  (let [lhs (dtype-dt/unpack lhs)
        rhs (dtype-dt/unpack rhs)
        lhs-dtype (collapse-date-datatype lhs)
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


(defmacro ^:private declare-getters
  []
  `(do
     ~@(->> (concat (keys dtype-dt/keyword->temporal-field)
                    [:milliseconds :nanoseconds])
            (map (fn [k]
                   `(defn ~(symbol (str "get-" (name k)))
                      [~'item]
                      (if (dtype-dt/duration-datatype?
                           (dtype-base/get-datatype ~'item))
                        (perform-duration-getter ~'item ~k)
                        (perform-int64-getter ~'item ~k))))))))


(declare-getters)


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


(defn- local-date-datatype?
  [dtype]
  (or (= dtype :local-date)
      (= dtype :packed-local-date)))


(defn plus-temporal-amount
  [lhs rhs]
  (cond
    (local-date-datatype? (dtype-base/get-datatype lhs))
    (plus-days lhs (dfn/round (get-days rhs)))
    (local-date-datatype? (dtype-base/get-datatype rhs))
    (plus-days rhs (dfn/round (get-days lhs)))
    :else
    (perform-commutative-temporal-amount-op
     lhs rhs :plus-temporal-amount)))


(defn minus-temporal-amount
  [lhs rhs]
  (perform-non-commutative-temporal-amount-op
   lhs rhs :minus-temporal-amount))


(defn plus-duration
  [lhs rhs]
  (let [lhs-dtype (dtype-base/get-datatype lhs)]
    (cond
      (or (= :duration lhs-dtype)
          (= :packed-duration lhs-dtype))
      (perform-duration-numeric-op lhs rhs :duration-plus-duration)
      (= :packed-duration dtype-base/get-datatype rhs)
      (plus-temporal-amount lhs (dtype-dt/unpack rhs))
      :else
      (plus-temporal-amount lhs rhs))))


(defn minus-duration
  [lhs rhs]
  (cond
    (= :duration (dtype-base/get-datatype lhs))
    (perform-duration-numeric-op lhs rhs :duration-minus-duration)
    (= :packed-duration (dtype-base/get-datatype lhs))
    (if (= :packed-duration (dtype-base/get-datatype rhs))
      (perform-duration-numeric-op lhs rhs :minus-nanoseconds)
      (dtype-dt/pack (plus-duration (dtype-dt/unpack lhs) rhs)))
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


(defn ->milliseconds
  "Convert a datatype to either epoch-seconds with an implied zero no time offset"
  [data]
  (let [datatype (dtype-base/get-datatype data)]
    (when-not (dtype-dt/datetime-datatype? datatype)
      (throw (Exception. (format "Invalid datatype for datetime operation: %s"
                                 datatype))))
    (if (dtype-dt/millis-datatypes datatype)
      (get-milliseconds data)
      (get-epoch-milliseconds data))))


(defn milliseconds->datetime
  "Vectorized Conversion of milliseconds to a given datetime datatype using defaults.
  Specialized conversions for particular datatypes also available as overrides or
  tech.v2.datatype.datetime/milliseconds-since-epoch->X where X can be:
  local-date local-date-time zoned-date-time."
  [datatype milli-data]
  (when-not (dtype-dt/datetime-datatype? datatype)
    (throw (Exception. (format "Datatype is not a datetime datatype: %s" datatype))))

  (let [unpacked-dt (dtype-dt/unpack-datatype datatype)
        packed? (dtype-dt/packed-datatype? datatype)
        conv-fn #(dtype-dt/from-milliseconds % unpacked-dt)
        retval
        (case (arg->arg-type milli-data)
          :scalar (conv-fn milli-data)
          :iterable (unary-op/unary-iterable-map {:datatype unpacked-dt}
                                                 conv-fn milli-data)
          :reader (unary-op/unary-reader-map {:datatype unpacked-dt}
                                             conv-fn milli-data))]
    (println "INMETHOD" (dtype-base/get-datatype retval) unpacked-dt)
    (if packed?
      (dtype-dt/pack retval)
      retval)))


(defn- ensure-reader
  [item]
  (if (= :scalar (arg->arg-type item))
    (const-rdr/make-const-reader item (dtype-base/get-datatype item))
    (dtype-proto/->reader item {})))


(defn local-date->milliseconds-since-epoch
  "Vectorized version of dtype-dt/local-date->milliseconds-since-epoch."
  ([data local-time-or-milli-offset zone-id-or-offset]
   (let [data (dtype-dt/unpack data)
         argtypes (set (map arg->arg-type [data local-time-or-milli-offset
                                           zone-id-or-offset]))
         conv-fn #(-> (dtype-dt/local-date->instant %1 %2 %3)
                      (dtype-dt/instant->milliseconds-since-epoch))]
     (cond
       (= argtypes #{:scalar})
       (conv-fn data local-time-or-milli-offset zone-id-or-offset)
       ;;if any of the three arguments are iterable
       (argtypes :iterable)
       (let [data (dtype-iter/->iterable data)
             lt (dtype-iter/->iterable local-time-or-milli-offset)
             zid (dtype-iter/->iterable zone-id-or-offset)]
         (map conv-fn data lt zid))
       :else
       (let [
             data (ensure-reader data)
             lt (ensure-reader local-time-or-milli-offset)
             zid-or-off (ensure-reader zone-id-or-offset)
             n-elems (long (clojure.core/min (dtype-base/ecount data)
                                             (dtype-base/ecount lt)
                                             (dtype-base/ecount zid-or-off)))]
         (reify
           tech.v2.datatype.LongReader
           (getDatatype [rdr] :epoch-milliseconds)
           (lsize [rdr] n-elems)
           (read [rdr idx]
             (long (conv-fn (data idx) (lt idx) (zid-or-off idx)))))))))
  ([data local-time-or-milli-offset]
   (local-date->milliseconds-since-epoch data local-time-or-milli-offset
                                         (dtype-dt/utc-zone-id)))
  ([data]
   (->milliseconds data)))


(defn vectorized-dispatch-2
  [arg1 arg2 scalar-fn reader-fn]
  (let [arg1 (dtype-dt/unpack arg1)
        arg2 (dtype-dt/unpack arg2)
        argtypes (set (map arg->arg-type [arg1 arg2]))]
     (cond
       (= argtypes #{:scalar})
       (scalar-fn arg1 arg2)
       ;;if any of the three arguments are iterable
       (argtypes :iterable)
       (let [arg1 (dtype-iter/->iterable arg1)
             arg2 (dtype-iter/->iterable arg2)]
         (map scalar-fn arg1 arg2))
       :else
       (let [arg1 (ensure-reader arg1)
             arg2 (ensure-reader arg2)
             n-elems (long (clojure.core/min (dtype-base/ecount arg1)
                                             (dtype-base/ecount arg2)))]
         (reader-fn arg1 arg2 n-elems)))))


(defn local-date-time->milliseconds-since-epoch
  "Vectorized version of dtype-dt/local-date-time->milliseconds-since-epoch."
  ([data zone-id-or-offset]
   (let [conv-fn #(-> (dtype-dt/local-date-time->instant %1 %2)
                      (dtype-dt/instant->milliseconds-since-epoch))]
     (vectorized-dispatch-2
      data zone-id-or-offset conv-fn
      (fn [data zone-id-or-offset n-elems]
        (reify
          tech.v2.datatype.LongReader
          (getDatatype [rdr] :epoch-milliseconds)
          (lsize [rdr] (long n-elems))
          (read [rdr idx]
            (long (conv-fn (data idx) (zone-id-or-offset idx)))))))))
  ([data] (->milliseconds data)))


(defn milliseconds-since-epoch->local-date-time
  "Vectorized version of datetime/milliseconds-since-epoch->local-date-time"
  ([millis-data zone-id]
   (let [conv-fn #(dtype-dt/milliseconds-since-epoch->local-date-time %1 %2)]
     (vectorized-dispatch-2
      millis-data zone-id conv-fn
      (fn [millis-data zone-id n-elems]
        (reify ObjectReader
          (getDatatype [rdr] :local-date-time)
          (lsize [rdr] (long n-elems))
          (read [rdr idx] (conv-fn (millis-data idx) (zone-id idx))))))))
  ([millis-data]
   (milliseconds->datetime :local-date-time millis-data)))


(defn milliseconds-since-epoch->zoned-date-time
  "Vectorized version of datetime/milliseconds-since-epoch->local-date-time"
  ([millis-data zone-id]
   (let [conv-fn #(dtype-dt/milliseconds-since-epoch->zoned-date-time %1 %2)]
     (vectorized-dispatch-2
      millis-data zone-id conv-fn
      (fn [millis-data zone-id n-elems]
        (reify ObjectReader
          (getDatatype [rdr] :zoned-date-time)
          (lsize [rdr] (long n-elems))
          (read [rdr idx] (conv-fn (millis-data idx) (zone-id idx))))))))
  ([millis-data]
   (milliseconds->datetime :zoned-date-time millis-data)))


(defn milliseconds-since-epoch->local-date
  "Vectorized version of datetime/milliseconds-since-epoch->local-date-time"
  ([millis-data zone-id]
   (let [conv-fn #(dtype-dt/milliseconds-since-epoch->local-date %1 %2)]
     (vectorized-dispatch-2
      millis-data zone-id conv-fn
      (fn [millis-data zone-id n-elems]
        (reify ObjectReader
          (getDatatype [rdr] :local-date)
          (lsize [rdr] (long n-elems))
          (read [rdr idx] (conv-fn (millis-data idx) (zone-id idx))))))))
  ([millis-data]
   (milliseconds->datetime :local-date millis-data)))


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
         numeric-data (->milliseconds data)
         unpacked-datatype (dtype-dt/unpack-datatype datatype)
         value-stats (if (dtype-dt/duration-datatype? datatype)
                       #{:min :max :mean :median :standard-deviation
                         :quartile-1 :quartile-3}
                       #{:min :max :mean :median :quartile-1 :quartile-3})
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
     (millisecond-descriptive-stats data #{:min :mean :max :standard-deviation
                                           :quartile-1 :quartile-3})
     (millisecond-descriptive-stats data #{:min :mean :max
                                           :quartile-1 :quartile-3}))))


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
