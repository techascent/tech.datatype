(ns tech.v2.datatype.datetime
  "Conversion routines and minimal support for date-time and packed date-time."
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.object-datatypes :as dtype-obj]
            [tech.v2.datatype.argtypes :as argtypes]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.pprint :as dtype-pp]
            [primitive-math :as pmath])
  (:import [java.time ZoneId ZoneOffset
            Instant ZonedDateTime OffsetDateTime
            LocalDate LocalDateTime LocalTime
            OffsetTime]
           [java.time.temporal ChronoUnit Temporal ChronoField
            WeekFields]
           [java.util Date Iterator Locale]
           [it.unimi.dsi.fastutil.bytes ByteIterator]
           [it.unimi.dsi.fastutil.shorts ShortIterator]
           [it.unimi.dsi.fastutil.ints IntIterator]
           [it.unimi.dsi.fastutil.longs LongIterator]
           [it.unimi.dsi.fastutil.floats FloatIterator]
           [it.unimi.dsi.fastutil.doubles DoubleIterator]
           [tech.v2.datatype
            PackedInstant PackedLocalDate
            PackedLocalTime PackedLocalDateTime
            ObjectReader LongReader IntReader
            IterHelpers$LongIterConverter
            IterHelpers$IntIterConverter
            IterHelpers$ObjectIterConverter]))


(set! *warn-on-reflection* true)


(def keyword->chrono-unit
  {:years ChronoUnit/YEARS
   :months ChronoUnit/MONTHS
   :weeks ChronoUnit/WEEKS
   :days ChronoUnit/DAYS
   :hours ChronoUnit/HOURS
   :minuts ChronoUnit/MINUTES
   :seconds ChronoUnit/SECONDS
   :milliseconds ChronoUnit/MILLIS})


(def keyword->temporal-field
  {:years ChronoField/YEAR
   :months ChronoField/MONTH_OF_YEAR
   :days ChronoField/DAY_OF_MONTH
   :day-of-year ChronoField/DAY_OF_YEAR
   :day-of-week ChronoField/DAY_OF_WEEK
   ;;Locale sensitive
   :week-of-year (.. (WeekFields/of (Locale/getDefault)) weekOfWeekBasedYear)
   :iso-week-of-year (.. (WeekFields/ISO) weekOfWeekBasedYear)
   :iso-day-of-week (.. (WeekFields/ISO) dayOfWeek)
   :epoch-days ChronoField/EPOCH_DAY
   :hours ChronoField/HOUR_OF_DAY
   :minutes ChronoField/MINUTE_OF_HOUR
   :seconds ChronoField/SECOND_OF_MINUTE
   :epoch-seconds ChronoField/INSTANT_SECONDS
   :milliseconds ChronoField/MILLI_OF_SECOND})


(defn seconds-in-day
  ^long []
  PackedLocalTime/SECONDS_PER_DAY)


(defn seconds-in-hour
  ^long []
  PackedLocalTime/SECONDS_PER_HOUR)


(defn seconds-in-minute
  ^long []
  PackedLocalTime/SECONDS_PER_MINUTE)


(defn nanoseconds-in-millisecond
  ^long []
  ;;(long 1e6)
  1000000)

(defn milliseconds-in-week
  ^long []
  60480000)

(defn milliseconds-in-day
  ^long []
  86400000)

(defn milliseconds-in-hour
  ^long []
  3600000)

(defn milliseconds-in-minute
  ^long []
  60000)

(defn milliseconds-in-second
  ^long []
  1000)

(defn system-zone-id
  ^ZoneId []
  (ZoneId/systemDefault))


(defn system-zone-offset
  ^ZoneOffset []
  (.. (OffsetDateTime/now) getOffset))


(defn zone-offset->seconds
  ^long [^ZoneOffset zof]
  (.getTotalSeconds zof))

(defn seconds-since-epoch->java-date
  ^Date [seconds]
  (-> (long  seconds)
      (* 1000)
      (Date.)))


(defn java-date->seconds-since-epoch
  (^long [^Date java-date]
   (long (quot (.getTime java-date) 1000)))
  (^long []
   (java-date->seconds-since-epoch (Date.))))


(defn milliseconds-since-epoch->java-date
  ^Date [milliseconds]
  (-> milliseconds (long) (Date.)))


(defn java-date->milliseconds-since-epoch
  (^long [^Date java-date]
   (long (.getTime java-date)))
  (^long []
   (java-date->milliseconds-since-epoch (Date.))))


(defn milliseconds-since-epoch->instant
  (^Instant [arg]
   (Instant/ofEpochMilli (long arg)))
  (^Instant []
   (Instant/now)))


(defn seconds-since-epoch->instant
  ^Instant [seconds]
  (milliseconds-since-epoch->instant (* (long seconds) 1000)))


(defn instant
  (^Instant []
   (Instant/now))
  (^Instant [arg]
   (if (instance? Instant arg)
     arg
     (milliseconds-since-epoch->instant arg))))


(defn instant->milliseconds-since-epoch
  ^long [^Instant instant]
  (.toEpochMilli instant))


(defn instant->seconds-since-epoch
  ^long [^Instant instant]
  (quot (.toEpochMilli instant)
        1000))


(defn instant->zoned-date-time
  (^ZonedDateTime [^Instant inst ^ZoneId zid]
   (.atZone inst zid))
  (^ZonedDateTime [^Instant inst]
   (instant->zoned-date-time inst (system-zone-id))))


(defn zoned-date-time->instant
  ^Instant [^ZonedDateTime zid]
  (.toInstant zid))


(defn zoned-date-time->milliseconds-since-epoch
  ^long [^ZonedDateTime zid]
  (-> (zoned-date-time->instant zid)
      (instant->milliseconds-since-epoch)))


(defn zoned-date-time
  (^ZonedDateTime []
   (ZonedDateTime/now))
  (^ZonedDateTime [arg]
   (if (instance? ZonedDateTime arg)
     arg
     (-> (milliseconds-since-epoch->instant)
         (instant->zoned-date-time)))))


(defn instant->offset-date-time
  (^OffsetDateTime [^Instant inst ^ZoneOffset offset]
   (.atOffset inst offset))
  (^OffsetDateTime [^Instant inst]
   (.atOffset inst (system-zone-offset)))
  (^OffsetDateTime []
   (OffsetDateTime/now)))


(defn offset-date-time->instant
  ^Instant [^OffsetDateTime of]
  (.toInstant of))


(defn offset-date-time->milliseconds-since-epoch
  ^long [^OffsetDateTime of]
  (-> (offset-date-time->instant of)
      (instant->milliseconds-since-epoch)))


(defn offset-date-time
  (^OffsetDateTime []
   (OffsetDateTime/now))
  (^OffsetDateTime [arg]
   (if (instance? OffsetDateTime arg)
     arg
     (-> (milliseconds-since-epoch->instant arg)
         (instant->offset-date-time)))))


(defn instant->local-date-time
  (^LocalDateTime [^Instant inst ^ZoneId zone-id]
   (LocalDateTime/ofInstant inst zone-id))
  (^LocalDateTime [^Instant inst]
   (LocalDateTime/ofInstant inst (ZoneId/systemDefault))))


(defn local-date-time->instant
  (^Instant [^LocalDateTime ldt ^ZoneOffset offset]
   (.toInstant ldt offset))
  (^Instant [^LocalDateTime ldt]
   (local-date-time->instant ldt (system-zone-offset))))


(defn local-date-time
  (^LocalDateTime []
   (LocalDateTime/now))
  (^LocalDateTime [arg]
   (if (instance? LocalDateTime arg)
     arg
     (-> (milliseconds-since-epoch->instant arg)
         (instant->local-date-time)))))


(defn milliseconds-since-epoch->local-date-time
  ^LocalDateTime [millis]
  (local-date-time millis))


(defn local-date-time->milliseconds-since-epoch
  ^long [^LocalDateTime ldt]
  (-> (local-date-time->instant ldt)
      (instant->milliseconds-since-epoch)))


(declare local-date-time->local-time
         local-date-time->local-date
         local-time->local-date-time)

(defn local-time
  (^LocalTime []
   (LocalTime/now))
  (^LocalTime [arg]
   (if (instance? LocalTime arg)
     arg
     (-> (local-date-time arg)
         (local-date-time->local-time)))))


(defn milliseconds-since-epoch->local-time
  ^LocalTime [millis]
  (local-time millis))


(defn local-time->instant
  (^Instant [^LocalTime lt]
   (-> (local-time->local-date-time lt)
       (local-date-time->instant)))
  (^Instant [^LocalTime lt ^LocalDate ld]
   (-> (local-time->local-date-time lt ld)
       (local-date-time->instant))))


(defn local-time->seconds
  ^long [^LocalTime lt]
  (long (.toSecondOfDay lt)))


(defn local-time->milliseconds
  ^long [^LocalTime lt]
  (quot (.toNanoOfDay lt)
        (nanoseconds-in-millisecond)))


(defn seconds->local-time
  ^LocalTime [^long seconds]
  (LocalTime/ofSecondOfDay seconds))


(defn milliseconds->local-time
  ^LocalTime [^long milliseconds]
  (LocalTime/ofNanoOfDay (* milliseconds
                            (nanoseconds-in-millisecond))))


(defn local-date
  (^LocalDate []
   (LocalDate/now))
  (^LocalDate [arg]
   (if (instance? LocalDate arg)
     arg
     (-> (local-date-time arg)
         (local-date-time->local-date)))))


(defn milliseconds-since-epoch->local-date
  ^LocalDate [millis]
  (local-date millis))


(defn local-date-time->local-date
  ^LocalDate [^LocalDateTime ldt]
  (.toLocalDate ldt))


(defn local-date->local-date-time
  (^LocalDateTime [^LocalDate ld ^LocalTime time]
   (.atTime ld ^LocalTime time))
  (^LocalDateTime [^LocalDate ld]
   (local-date->local-date-time ld (LocalTime/now))))


(defn local-date->instant
  (^Instant [^LocalDate ld ^LocalTime lt]
   (-> (local-date->local-date-time ld lt)
       (local-date-time->instant)))
  (^Instant [^LocalDate ld]
   (-> (local-date->local-date-time ld)
       (local-date-time->instant))))


(defn local-date->milliseconds-since-epoch
  (^long [^LocalDate ld ^LocalTime lt]
   (-> (local-date->local-date-time ld lt)
       (local-date-time->milliseconds-since-epoch)))
  (^long [^LocalDate ld]
   (-> (local-date->local-date-time ld)
       (local-date-time->milliseconds-since-epoch))))


(defn local-date-time->local-time
  ^LocalTime [^LocalDateTime ldt]
  (.toLocalTime ldt))


(defn local-time->local-date-time
  (^LocalDateTime [^LocalTime ldt ^LocalDate day]
   (local-date->local-date-time day ldt))
  (^LocalDateTime [^LocalTime ldt]
   (local-date->local-date-time (local-date) ldt)))


(defn offset-time
  ^OffsetTime []
  (OffsetTime/now))


(defn local-date->offset-date-time
  (^OffsetDateTime [^LocalDate ld ^OffsetTime of]
   (.atTime ld of))
  (^OffsetDateTime [^LocalDate ld]
   (.atTime ld (offset-time))))


(defn- make-obj-constructor
  [cons-fn]
  (fn
    ([] (cons-fn))
    ([arg] (when arg (cons-fn arg)))))


(dtype-obj/add-object-datatype Instant :instant (make-obj-constructor instant))
(dtype-obj/add-object-datatype LocalDate :local-date (make-obj-constructor local-date))
(dtype-obj/add-object-datatype LocalDateTime :local-date-time
                               (make-obj-constructor local-date-time))
(dtype-obj/add-object-datatype LocalTime :local-time
                               (make-obj-constructor local-time))
(dtype-obj/add-object-datatype OffsetDateTime :offset-date-time
                               (make-obj-constructor offset-date-time))
(dtype-obj/add-object-datatype ZonedDateTime :zoned-date-time
                               (make-obj-constructor zoned-date-time))
(dtype-obj/add-object-datatype OffsetDateTime :offset-date-time
                               (make-obj-constructor offset-date-time))
(dtype-obj/add-object-datatype OffsetTime :offset-time
                               (make-obj-constructor offset-time))



(defn as-instant ^Instant [item] item)
(defn as-local-time ^LocalTime [item] item)
(defn as-local-date ^LocalDate [item] item)
(defn as-local-date-time ^LocalDateTime [item] item)


(defmacro datatype->cast-fn
  [src-dtype dst-dtype val]
  (if (= src-dtype dst-dtype)
    val
    (case dst-dtype
      :instant `(as-instant ~val)
      :local-time `(as-local-time ~val)
      :local-date `(as-local-date ~val)
      :local-date-time `(as-local-date-time ~val))))


;;datatypes are packed into dense integer objects.  There are static methods on
;;the packing objects to still do some manipulations while packed.  When done en mass,
;;these manipulations will be much faster than when converted to java types and
;;the manipulation done there.

(defmacro compile-time-pack
  [item dtype]
  (case dtype
    :instant `(PackedInstant/pack ~item)
    :local-time `(PackedLocalTime/pack ~item)
    :local-date `(PackedLocalDate/pack ~item)
    :local-date-time `(PackedLocalDateTime/pack (as-local-date-time ~item))))


(defmacro compile-time-unpack
  [item dtype]
  (case dtype
    :instant `(PackedInstant/asInstant (pmath/long ~item))
    :local-time `(PackedLocalTime/asLocalTime (pmath/int ~item))
    :local-date `(PackedLocalDate/asLocalDate (pmath/int ~item))
    :local-date-time `(PackedLocalDateTime/asLocalDateTime (pmath/long ~item))))


(def packable-datatypes
  [:local-date :local-time :instant :local-date-time])


(def packable-datatype->primitive-datatype
  {:local-date :int32
   :local-time :int32
   :local-date-time :int64
   :instant :int64})


(defn packed-type->unpacked-type
  [datatype]
  (case datatype
    :packed-instant :instant
    :packed-local-date-time :local-date-time
    :packed-local-date :local-date
    :packed-local-time :local-time))


(defn unpacked-type->packed-type
  [datatype]
  (case datatype
    :instant :packed-instant
    :local-date-time :packed-local-date-time
    :local-date :packed-local-date
    :local-time :packed-local-time))



(def packed-datatypes
  #{:packed-instant
    :packed-local-date-time
    :packed-local-date
    :packed-local-time})


(defn packed-datatype?
  [datatype]
  (boolean (packed-datatypes datatype)))


;;As our packed types are really primitive aliases we tell the datatype system about
;;this so that other machinery can get to the base datatype.
(->> packable-datatype->primitive-datatype
     (mapv (fn [[k v]]
             (casting/alias-datatype! (keyword (str "packed-" (name k))) v))))


(defmacro define-packing-operations
  [packed-dtype]
  (let [prim-dtype (get packable-datatype->primitive-datatype packed-dtype)
        packed-name (name packed-dtype)
        packed-name-kwd (keyword (str "packed-" packed-name))
        iter->pack-sym (symbol (format "%s-iterable->packed-%s-iterable"
                                       packed-name packed-name))
        reader->pack-sym (symbol (format "%s-reader->packed-%s-reader"
                                         packed-name packed-name))
        iter->unpack-sym (symbol (format "packed-%s-iterable->%s-iterable"
                                         packed-name packed-name))
        reader->unpack-sym (symbol (format "packed-%s-reader->%s-reader"
                                           packed-name packed-name))]
    `(do
       (defn ~iter->pack-sym
         [^Iterable iterable#]
         (reify
           dtype-proto/PDatatype
           (get-datatype [itr#] ~packed-name-kwd)
           Iterable
           (iterator [itr#]
             (let [^Iterator src-iter# (.iterator iterable#)]
               (typecast/datatype->iter-helper
                ~prim-dtype
                (reify
                  ~(typecast/datatype->fastutil-iter-type prim-dtype)
                  (hasNext [itr] (.hasNext src-iter#))
                  (~(typecast/datatype->iter-next-fn-name prim-dtype) [itr]
                   (compile-time-pack (.next src-iter#) ~packed-dtype)))
                ~packed-name-kwd)))))

       (defn ~reader->pack-sym
         ^LongReader [reader#]
         (let [reader# (typecast/datatype->reader :object reader#)]
           (reify ~(typecast/datatype->reader-type prim-dtype)
             (getDatatype [rdr#] ~packed-name-kwd)
             (lsize [rdr#] (.lsize reader#))
             (read [rdr# idx#] (-> (.read reader# idx#)
                                   (compile-time-pack ~packed-dtype))))))

       (defn ~(symbol (format "pack-%s" packed-name))
         [item#]
         (let [argtype# (argtypes/arg->arg-type item#)]
           (case argtype#
             :scalar (compile-time-pack item# ~packed-dtype)
             :iterable (~iter->pack-sym item#)
             :reader (~reader->pack-sym item#))))

       (defn ~iter->unpack-sym
         ^Iterable [^Iterable packed-inst-iterable#]
         (reify
           dtype-proto/PDatatype
           (get-datatype [itr] ~packed-dtype)
           Iterable
           (iterator [tr]
             (let [^Iterator src-iter# (.iterator ^Iterable packed-inst-iterable#)]
               (-> (reify Iterator
                     (hasNext [iter] (.hasNext src-iter#))
                     (next [iter] (compile-time-unpack (.next src-iter#)
                                                       ~packed-dtype)))
                   (IterHelpers$ObjectIterConverter. ~packed-dtype))))))

       (defn ~reader->unpack-sym
         ^ObjectReader [reader#]
         (let [reader# (typecast/datatype->reader :int64 reader#)]
           (reify ObjectReader
             (getDatatype [rdr#] ~packed-dtype)
             (lsize [rdr#] (.lsize reader#))
             (read [rdr# idx#] (-> (.read reader# idx#)
                                   (compile-time-unpack ~packed-dtype))))))

       (defn ~(with-meta (symbol (format "unpack-%s" packed-name))
                {:tag Temporal})
         [item#]
         (let [argtype# (argtypes/arg->arg-type item#)]
           (case argtype#
             :scalar (compile-time-unpack item# ~packed-dtype)
             :iterable (~iter->unpack-sym item#)
             :reader (~reader->unpack-sym item#)))))))


(define-packing-operations :instant)
(define-packing-operations :local-date)
(define-packing-operations :local-time)
(define-packing-operations :local-date-time)

(defn packed-local-date-time->milliseconds-since-epoch
  ^long [^long packed-date-time]
  (-> (unpack-local-date-time)
      (local-date-time->milliseconds-since-epoch)))


(defn packed-local-date->milliseconds-since-epoch
  ^long [^long packed-date]
  (-> (unpack-local-date)
      (local-date->milliseconds-since-epoch)))

(defn packed-instant->milliseconds-since-epoch
  ^long [^long packed-instant]
  (-> (unpack-instant)
      (instant->milliseconds-since-epoch)))


(defn packed-local-time->milliseconds
  ^long [^long packed-time]
  (-> (unpack-local-time)
      (local-time->milliseconds)))


(def packed-aliases (->> packable-datatypes
                         (map (fn [name-kwd]
                                (keyword (format "packed-%s"
                                                 (name name-kwd)))))
                         set))


(defn collapse-date-datatype
  [item]
  (let [item-dtype (dtype-base/get-datatype item)]
    (cond
      ;;don't collapse packed-instant
      (packed-aliases item-dtype) item-dtype
      ;;But do collapse any other aliases.
      (casting/numeric-type? (casting/safe-flatten item-dtype)) :number
      :else
      (if (= item-dtype :object)
        (if-let [fitem (first item)]
          (collapse-date-datatype fitem)
          item-dtype)
        item-dtype))))


(defn pack
  [item]
  (case (collapse-date-datatype item)
    :instant
    (case (argtypes/arg->arg-type item)
      :scalar (pack-instant item)
      :iterable (instant-iterable->packed-instant-iterable item)
      :reader (instant-reader->packed-instant-reader item))
    :local-date-time
    (case (argtypes/arg->arg-type item)
      :scalar (pack-local-date-time item)
      :iterable (local-date-time-iterable->packed-local-date-time-iterable item)
      :reader (local-date-time-reader->packed-local-date-time-reader item))
    :local-date
    (case (argtypes/arg->arg-type item)
      :scalar (pack-local-date item)
      :iterable (local-date-iterable->packed-local-date-iterable item)
      :reader (local-date-reader->packed-local-date-reader item))
    :local-time
    (case (argtypes/arg->arg-type item)
      :scalar (pack-local-time item)
      :iterable (local-time-iterable->packed-local-time-iterable item)
      :reader (local-time-reader->packed-local-time-reader item))))


(defn unpack
  [item]
  (case (collapse-date-datatype item)
    :packed-instant
    (case (argtypes/arg->arg-type item)
      :scalar (unpack-instant item)
      :iterable (packed-instant-iterable->instant-iterable item)
      :reader (packed-instant-reader->instant-reader item))
    :packed-local-date-time
    (case (argtypes/arg->arg-type item)
      :scalar (unpack-local-date-time item)
      :iterable (packed-local-date-time-iterable->local-date-time-iterable item)
      :reader (packed-local-date-time-reader->local-date-time-reader item))
    :packed-local-date
    (case (argtypes/arg->arg-type item)
      :scalar (unpack-local-date item)
      :iterable (packed-local-date-iterable->local-date-iterable item)
      :reader (packed-local-date-reader->local-date-reader item))
    :packed-local-time
    (case (argtypes/arg->arg-type item)
      :scalar (unpack-local-time item)
      :iterable (packed-local-time-iterable->local-time-iterable item)
      :reader (packed-local-time-reader->local-time-reader item))))


(defmethod dtype-pp/reader-printer :packed-instant
  [rdr]
  (unpack rdr))

(defmethod dtype-pp/reader-printer :packed-local-date-time
  [rdr]
  (unpack rdr))

(defmethod dtype-pp/reader-printer :packed-local-date
  [rdr]
  (unpack rdr))

(defmethod dtype-pp/reader-printer :packed-local-time
  [rdr]
  (unpack rdr))


(defprotocol PEpochMilliseconds
  (->milliseconds-since-epoch [arg]))


(extend-protocol PEpochMilliseconds
  Instant
  (->milliseconds-since-epoch [arg]
    (instant->milliseconds-since-epoch arg))
  ZonedDateTime
  (->milliseconds-since-epoch [arg]
    (zoned-date-time->milliseconds-since-epoch arg))
  OffsetDateTime
  (->milliseconds-since-epoch [arg]
    (offset-date-time->milliseconds-since-epoch arg))
  ;;And here we cheat
  LocalDateTime
  (->milliseconds-since-epoch [arg]
    (local-date-time->milliseconds-since-epoch arg))
  LocalDate
  (->milliseconds-since-epoch [arg]
    (local-date->milliseconds-since-epoch arg)))
