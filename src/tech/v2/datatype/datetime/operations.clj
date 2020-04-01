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
            [cljc.java-time.local-date :as local-date]
            [cljc.java-time.local-date-time :as local-date-time]
            [cljc.java-time.local-time :as local-time]
            [cljc.java-time.instant :as instant]
            [cljc.java-time.zoned-date-time :as zoned-date-time]
            [cljc.java-time.offset-date-time :as offset-date-time]
            [clojure.pprint :as pp])
  (:import [java.time ZoneId ZoneOffset
            Instant ZonedDateTime OffsetDateTime
            LocalDate LocalDateTime LocalTime
            OffsetTime]
           [java.time.temporal TemporalUnit ChronoUnit
            Temporal ChronoField]
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
       (into {})))


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


(defmacro ^:private make-packed-temporal-getter
  [datatype opname temporal-field]
  (let [src-dtype (dtype-dt/packed-type->unpacked-type datatype)]
    `(unary-op/make-unary-op
      (keyword (format "%s-get-%s" (name ~datatype) (name ~opname)))
      :int64
      (casting/datatype->unchecked-cast-fn
       :unknown
       ~(casting/datatype->host-type datatype)
       (-> (dtype-dt/compile-time-unpack ~'x ~src-dtype)
           (.getLong ~temporal-field))))))


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
              (casting/datatype->unchecked-cast-fn
               :unknown
               ~(casting/datatype->host-type datatype)
               (-> (dtype-dt/compile-time-unpack ~'x ~src-dtype)
                   (dtype-dt/->milliseconds-since-epoch))))})))


(def java-time-ops
  {:instant
   {:numeric-ops temporal-numeric-ops
    :int64-getters temporal-getters
    :boolean-ops
    {:< (boolean-op/make-boolean-binary-op
         :instant-before :instant
         (instant/is-before x y))
     :> (boolean-op/make-boolean-binary-op
         :instant-before :instant
         (instant/is-before x y))
     :<= (boolean-op/make-boolean-binary-op
          :instant-before :instant
          (or (instant/equals x y) (instant/is-before x y)))
     :>= (boolean-op/make-boolean-binary-op
          :instant-before :instant
          (or (instant/equals x y) (instant/is-after x y)))
     :== (boolean-op/make-boolean-binary-op
          :instant-== :instant
          (instant/equals x y))}
    :binary-ops
    {:min (binary-op/make-binary-op
           :instant-min :instant
           (if (and x y)
             (if (instant/is-before x y) x y)))
     :max (binary-op/make-binary-op
           :instant-max :instant
           (if (and x y)
             (if (instant/is-after x y) x y)))}
    :binary->int64-ops
    {:difference-milliseconds
     (binary-op/make-binary-op
      :instant-difference-millis :object
      (- (dtype-dt/instant->milliseconds-since-epoch x)
         (dtype-dt/instant->milliseconds-since-epoch y)))}}

   :zoned-date-time
   {:numeric-ops temporal-numeric-ops
    :int64-getters temporal-getters
    :boolean-ops
    {:< (boolean-op/make-boolean-binary-op
         :zoned-date-time-before :zoned-date-time
         (zoned-date-time/is-before x y))
     :> (boolean-op/make-boolean-binary-op
         :zoned-date-time-before :zoned-date-time
         (zoned-date-time/is-before x y))
     :<= (boolean-op/make-boolean-binary-op
          :zoned-date-time-before :zoned-date-time
          (or (zoned-date-time/equals x y) (zoned-date-time/is-before x y)))
     :>= (boolean-op/make-boolean-binary-op
          :zoned-date-time-before :zoned-date-time
          (or (zoned-date-time/equals x y) (zoned-date-time/is-after x y)))
     :== (boolean-op/make-boolean-binary-op
          :zoned-date-time-== :zoned-date-time
          (zoned-date-time/equals x y))}
    :binary-ops
    {:min (binary-op/make-binary-op
           :zoned-date-time-min :zoned-date-time
           (if (and x y)
             (if (zoned-date-time/is-before x y) x y)))
     :max (binary-op/make-binary-op
           :zoned-date-time-max :zoned-date-time
           (if (and x y)
             (if (zoned-date-time/is-after x y) x y)))}
    :binary->int64-ops
    {:difference-milliseconds
     (binary-op/make-binary-op
      :zoned-date-time-difference-millis :object
      (- (dtype-dt/zoned-date-time->milliseconds-since-epoch x)
         (dtype-dt/zoned-date-time->milliseconds-since-epoch y)))}}

   :offset-date-time
   {:numeric-ops temporal-numeric-ops
    :int64-getters temporal-getters
    :boolean-ops
    {:< (boolean-op/make-boolean-binary-op
         :offset-date-time-before :offset-date-time
         (offset-date-time/is-before x y))
     :> (boolean-op/make-boolean-binary-op
         :offset-date-time-before :offset-date-time
         (offset-date-time/is-before x y))
     :<= (boolean-op/make-boolean-binary-op
          :offset-date-time-before :offset-date-time
          (or (offset-date-time/equals x y) (offset-date-time/is-before x y)))
     :>= (boolean-op/make-boolean-binary-op
          :offset-date-time-before :offset-date-time
          (or (offset-date-time/equals x y) (offset-date-time/is-after x y)))
     :== (boolean-op/make-boolean-binary-op
          :offset-date-time-== :offset-date-time
          (offset-date-time/equals x y))}
    :binary-ops
    {:min (binary-op/make-binary-op
           :offset-date-time-min :offset-date-time
           (if (and x y)
             (if (offset-date-time/is-before x y) x y)))
     :max (binary-op/make-binary-op
           :offset-date-time-max :offset-date-time
           (if (and x y)
             (if (offset-date-time/is-after x y) x y)))}
    :binary->int64-ops
    {:difference-milliseconds
     (binary-op/make-binary-op
      :offset-date-time-difference-millis :object
      (- (dtype-dt/offset-date-time->milliseconds-since-epoch x)
         (dtype-dt/offset-date-time->milliseconds-since-epoch y)))}}

   :local-date-time
   {:numeric-ops temporal-numeric-ops
    :int64-getters temporal-getters
    :boolean-ops
    {:< (boolean-op/make-boolean-binary-op
         :local-date-time-before :local-date-time
         (local-date-time/is-before x y))
     :> (boolean-op/make-boolean-binary-op
         :local-date-time-before :local-date-time
         (local-date-time/is-before x y))
     :<= (boolean-op/make-boolean-binary-op
          :local-date-time-before :local-date-time
          (or (local-date-time/equals x y) (local-date-time/is-before x y)))
     :>= (boolean-op/make-boolean-binary-op
          :local-date-time-before :local-date-time
          (or (local-date-time/equals x y) (local-date-time/is-after x y)))
     :== (boolean-op/make-boolean-binary-op
          :local-date-time-== :local-date-time
          (local-date-time/equals x y))}
    :binary-ops
    {:min (binary-op/make-binary-op
           :local-date-time-min :local-date-time
           (if (and x y)
             (if (local-date-time/is-before x y) x y)))
     :max (binary-op/make-binary-op
           :local-date-time-max :local-date-time
           (if (and x y)
             (if (local-date-time/is-after x y) x y)))}
    :binary->int64-ops
    {:difference-milliseconds
     (binary-op/make-binary-op
      :local-date-time-difference-millis :object
      (- (dtype-dt/local-date-time->milliseconds-since-epoch x)
         (dtype-dt/local-date-time->milliseconds-since-epoch y)))}}


   :packed-local-date-time
   {:numeric-ops (make-packed-numeric-ops :packed-local-date-time)
    :int64-getters (make-packed-getters :packed-local-date-time)
    ;;Packed objects are built so the basic math ops work.
    :boolean-ops
    {:< (boolean-op/make-boolean-binary-op
         :packed-local-date-time-before :packed-local-date-time
         (clojure.core/< x y))
     :> (boolean-op/make-boolean-binary-op
         :packed-local-date-time-before :packed-local-date-time
         (clojure.core/> x y))
     :<= (boolean-op/make-boolean-binary-op
          :packed-local-date-time-before :packed-local-date-time
          (clojure.core/<= x y))
     :>= (boolean-op/make-boolean-binary-op
          :packed-local-date-time-before :packed-local-date-time
          (clojure.core/>= x y))
     :== (boolean-op/make-boolean-binary-op
          :packed-local-date-time-== :packed-local-date-time
          (clojure.core/== x y))}
    :binary-ops
    {:min (binary-op/make-binary-op
           :packed-local-date-time-min :packed-local-date-time
           (if (clojure.core/< x y) x y))
     :max (binary-op/make-binary-op
           :packed-local-date-time-max :packed-local-date-time
           (if (clojure.core/> x y) x y))}
    :binary->int64-ops
    {:difference-milliseconds
     (binary-op/make-binary-op
      :packed-local-date-time-difference-millis :int64
      (- (dtype-dt/packed-local-date-time->milliseconds-since-epoch x)
         (dtype-dt/packed-local-date-time->milliseconds-since-epoch y)))}}

   :local-date
   {:numeric-ops temporal-numeric-ops
    :int64-getters temporal-getters
    :boolean-ops
    {:< (boolean-op/make-boolean-binary-op
         :local-date-before :local-date
         (local-date/is-before x y))
     :> (boolean-op/make-boolean-binary-op
         :local-date-before :local-date
         (local-date/is-before x y))
     :<= (boolean-op/make-boolean-binary-op
          :local-date-before :local-date
          (or (local-date/equals x y) (local-date/is-before x y)))
     :>= (boolean-op/make-boolean-binary-op
          :local-date-before :local-date
          (or (local-date/equals x y) (local-date/is-after x y)))
     :== (boolean-op/make-boolean-binary-op
          :local-date-== :local-date
          (local-date/equals x y))}
    :binary-ops
    {:min (binary-op/make-binary-op
           :local-date-min :local-date
           (if (and x y)
             (if (local-date/is-before x y) x y)))
     :max (binary-op/make-binary-op
           :local-date-max :local-date
           (if (and x y)
             (if (local-date/is-after x y) x y)))}
    :binary->int64-ops
    {:difference-milliseconds
     (binary-op/make-binary-op
      :local-date-difference-millis :object
      (- (dtype-dt/local-date->milliseconds-since-epoch x)
         (dtype-dt/local-date->milliseconds-since-epoch y)))}}


   :packed-local-date
   {:numeric-ops (make-packed-numeric-ops :packed-local-date)
    :int64-getters (make-packed-getters :packed-local-date)
    :boolean-ops
    {:< (boolean-op/make-boolean-binary-op
         :packed-local-date-before :packed-local-date
         (clojure.core/< x y))
     :> (boolean-op/make-boolean-binary-op
         :packed-local-date-before :packed-local-date
         (clojure.core/> x y))
     :<= (boolean-op/make-boolean-binary-op
          :packed-local-date-before :packed-local-date
          (clojure.core/<= x y))
     :>= (boolean-op/make-boolean-binary-op
          :packed-local-date-before :packed-local-date
          (clojure.core/>= x y))
     :== (boolean-op/make-boolean-binary-op
          :packed-local-date-== :packed-local-date
          (clojure.core/== x y))}
    :binary-ops
    {:min (binary-op/make-binary-op
           :packed-local-date-min :packed-local-date
           (if (clojure.core/< x y) x y))
     :max (binary-op/make-binary-op
           :packed-local-date-max :packed-local-date
           (if (clojure.core/> x y) x y))}
    :binary->int64-ops
    {:difference-milliseconds
     (binary-op/make-binary-op
      :packed-local-date-difference-millis :object
      (- (dtype-dt/packed-local-date->milliseconds-since-epoch x)
         (dtype-dt/packed-local-date->milliseconds-since-epoch y)))}}

   :local-time
   {:numeric-ops temporal-numeric-ops
    :int64-getters temporal-getters
    :boolean-ops
    {:< (boolean-op/make-boolean-binary-op
         :local-time-before :local-time
         (local-time/is-before x y))
     :> (boolean-op/make-boolean-binary-op
         :local-time-before :local-time
         (local-time/is-before x y))
     :<= (boolean-op/make-boolean-binary-op
          :local-time-before :local-time
          (or (local-time/equals x y) (local-time/is-before x y)))
     :>= (boolean-op/make-boolean-binary-op
          :local-time-before :local-time
          (or (local-time/equals x y) (local-time/is-after x y)))
     :== (boolean-op/make-boolean-binary-op
          :local-time-== :local-time
          (local-time/equals x y))}
    :binary-ops
    {:min (binary-op/make-binary-op
           :local-time-min :local-time
           (if (and x y)
             (if (local-time/is-before x y) x y)))
     :max (binary-op/make-binary-op
           :local-time-max :local-time
           (if (and x y)
             (if (local-time/is-after x y) x y)))}
    :binary->int64-ops
    {:difference-milliseconds
     (binary-op/make-binary-op
      :local-time-difference-millis :object
      (- (dtype-dt/local-time->milliseconds x)
         (dtype-dt/local-time->milliseconds y)))}}


   :packed-local-time
   {:numeric-ops (make-packed-numeric-ops :packed-local-time)
    :int64-getters (make-packed-getters :packed-local-time)
    :boolean-ops
    {:< (boolean-op/make-boolean-binary-op
         :packed-local-time-before :packed-local-time
         (clojure.core/< x y))
     :> (boolean-op/make-boolean-binary-op
         :packed-local-time-before :packed-local-time
         (clojure.core/> x y))
     :<= (boolean-op/make-boolean-binary-op
          :packed-local-time-before :packed-local-time
          (clojure.core/<= x y))
     :>= (boolean-op/make-boolean-binary-op
          :packed-local-time-before :packed-local-time
          (clojure.core/>= x y))
     :== (boolean-op/make-boolean-binary-op
          :packed-local-time-== :packed-local-time
          (clojure.core/== x y))}
    :binary-ops
    {:min (binary-op/make-binary-op
           :packed-local-time-min :packed-local-time
           (if (clojure.core/< x y) x y))
     :max (binary-op/make-binary-op
           :packed-local-time-max :packed-local-time
           (if (clojure.core/> x y) x y))}
    :binary->int64-ops
    {:difference-milliseconds
     (binary-op/make-binary-op
      :packed-local-time-difference-millis :object
      (- (dtype-dt/packed-local-time->milliseconds x)
         (dtype-dt/packed-local-time->milliseconds y)))}}

   :packed-instant
   {:numeric-ops (make-packed-numeric-ops :packed-instant)
    :int64-getters (make-packed-getters :packed-instant)
    :boolean-ops
    {:< (boolean-op/make-boolean-binary-op
         :packed-instant-before :packed-instant
         (clojure.core/< x y))
     :> (boolean-op/make-boolean-binary-op
         :packed-instant-before :packed-instant
         (clojure.core/> x y))
     :<= (boolean-op/make-boolean-binary-op
          :packed-instant-before :packed-instant
          (clojure.core/<= x y))
     :>= (boolean-op/make-boolean-binary-op
          :packed-instant-before :packed-instant
          (clojure.core/>= x y))
     :== (boolean-op/make-boolean-binary-op
          :packed-instant-== :packed-instant
          (clojure.core/== x y))}
    :binary-ops
    {:min (binary-op/make-binary-op
           :packed-instant-min :packed-instant
           (if (clojure.core/< x y) x y))
     :max (binary-op/make-binary-op
           :packed-instant-max :packed-instant
           (if (clojure.core/> x y) x y))}
    :binary->int64-ops
    {:difference-milliseconds
     (binary-op/make-binary-op
      :packed-instant-difference-millis :object
      (- (dtype-dt/packed-instant->milliseconds-since-epoch x)
         (dtype-dt/packed-instant->milliseconds-since-epoch y)))}}})


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


(def date-datatypes #{:instant :local-date :local-date-time :local-time
                      :packed-instant :packed-local-date :packed-local-date-time
                      :packed-local-time :zoned-date-time :offset-date-time})

(defn- date-datatype?
  [dtype]
  (boolean
   (date-datatypes dtype)))


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
              (dtype-proto/->reader lhs {:datatype :int64})
              lhs)
        unary-op (get-in java-time-ops [lhs-dtype :int64-getters
                                        unary-op-name])
        result-dtype (if (#{:epoch-seconds :epoch-milliseconds} unary-op-name)
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
        any-date-datatype? (or (date-datatype? lhs-dtype)
                               (date-datatype? rhs-dtype))
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
        any-date-datatype? (date-datatype? lhs-dtype)
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


(defn- perform-boolean-op
  [lhs rhs opname]
  (let [lhs-dtype (collapse-date-datatype lhs)
        rhs-dtype (collapse-date-datatype rhs)
        any-date-datatype? (or (date-datatype? lhs-dtype)
                               (date-datatype? rhs-dtype))
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
        any-date-datatype? (or (date-datatype? lhs-dtype)
                               (date-datatype? rhs-dtype))
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
        any-date-datatype? (date-datatype? lhs-dtype)
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
        any-date-datatype? (or (date-datatype? lhs-dtype)
                               (date-datatype? rhs-dtype))
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


(defn get-epoch-milliseconds
  [item]
  (perform-int64-getter item :epoch-milliseconds))


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
