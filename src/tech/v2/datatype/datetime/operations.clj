(ns tech.v2.datatype.datetime.operations
  (:require [tech.v2.datatype.datetime
             :refer [collapse-date-datatype]
             :as dtype-dt]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.readers.const :refer [make-const-reader]]
            [tech.v2.datatype.iterable.const :refer [make-const-iterable]]
            [tech.v2.datatype.argtypes :refer [arg->arg-type]]
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

            [tech.v2.datatype.datetime.packed-local-date-time
             :as packed-local-date-time]
            [tech.v2.datatype.datetime.packed-local-date
             :as packed-local-date]
            [tech.v2.datatype.datetime.packed-local-time
             :as packed-local-time])
  (:import [java.time ZoneId ZoneOffset
            Instant ZonedDateTime OffsetDateTime
            LocalDate LocalDateTime LocalTime
            OffsetTime]
           [java.time.temporal TemporalUnit ChronoUnit]
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

(def java-time-ops
  {:instant
   {:numeric-ops
    {:add-years
     (binary-op/make-binary-op
      :instant-add-weeks :instant
      (when x
        (.plus ^Instant x
               (* 7 (dtype-dt/seconds-in-day)
                  (long (Math/round
                         (if y (double y) 0.0))))
               ChronoUnit/YEARS)))
     :add-months
     (binary-op/make-binary-op
      :instant-add-weeks :instant
      (when x
        (.plus ^Instant x
               (* 7 (dtype-dt/seconds-in-day)
                  (long (Math/round
                         (if y (double y) 0.0))))
               ChronoUnit/MONTHS)))
     :add-weeks
     (binary-op/make-binary-op
      :instant-add-weeks :instant
      (when x
        (instant/plus-seconds
         x
         (* 7 (dtype-dt/seconds-in-day)
            (long (Math/round
                   (if y (double y) 0.0)))))))
     :add-days
     (binary-op/make-binary-op
      :instant-add-day :instant
      (when x
        (instant/plus-seconds
         x
         (long (Math/round
                (* (if y (double y) 0.0)
                   (dtype-dt/seconds-in-day)))))))
     :add-hours
     (binary-op/make-binary-op
      :instant-add-hour :instant
      (when x
        (instant/plus-millis
         x
         (long (Math/round
                (* (if y (double y) 0.0)
                   (dtype-dt/milliseconds-in-hour)))))))
     :add-minutes
     (binary-op/make-binary-op
      :instant-add-minute :instant
      (when x
        (instant/plus-millis
         x
         (long (Math/round
                (* (if y (double y) 0.0)
                   (dtype-dt/milliseconds-in-minute)))))))
     :add-seconds
     (binary-op/make-binary-op
      :instant-add-second :instant
      (when x
        (instant/plus-seconds
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-milliseconds
     (binary-op/make-binary-op
      :instant-add-millisecond :instant
      (when x
        (instant/plus-millis
         x
         (long (Math/round
                (if y (double y) 0.0))))))}
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
   {:numeric-ops
    {:add-years
     (binary-op/make-binary-op
      :zoned-date-time-add-day :zoned-date-time
      (when x
        (zoned-date-time/plus-years
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-months
     (binary-op/make-binary-op
      :zoned-date-time-add-day :zoned-date-time
      (when x
        (zoned-date-time/plus-months
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-weeks
     (binary-op/make-binary-op
      :zoned-date-time-add-day :zoned-date-time
      (when x
        (zoned-date-time/plus-weeks
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-days
     (binary-op/make-binary-op
      :zoned-date-time-add-day :zoned-date-time
      (when x
        (zoned-date-time/plus-seconds
         x
         (long (Math/round
                (* (if y (double y) 0.0)
                   (dtype-dt/seconds-in-day)))))))
     :add-hours
     (binary-op/make-binary-op
      :zoned-date-time-add-hour :zoned-date-time
      (when x
        (zoned-date-time/plus-seconds
         x
         (long (Math/round
                (* (if y (double y) 0.0)
                   (dtype-dt/seconds-in-hour)))))))
     :add-minutes
     (binary-op/make-binary-op
      :zoned-date-time-add-minute :zoned-date-time
      (when x
        (zoned-date-time/plus-seconds
         x
         (long (Math/round
                (* (if y (double y) 0.0)
                   (dtype-dt/seconds-in-minute)))))))
     :add-seconds
     (binary-op/make-binary-op
      :zoned-date-time-add-second :zoned-date-time
      (when x
        (zoned-date-time/plus-seconds
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-milliseconds
     (binary-op/make-binary-op
      :zoned-date-time-add-millisecond :zoned-date-time
      (when x
        (zoned-date-time/plus-nanos
         x
         (* (dtype-dt/nanoseconds-in-millisecond)
            (long (Math/round
                   (if y (double y) 0.0)))))))}
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
   {:numeric-ops
    {:add-years
     (binary-op/make-binary-op
      :offset-date-time-add-day :offset-date-time
      (when x
        (offset-date-time/plus-years
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-months
     (binary-op/make-binary-op
      :offset-date-time-add-day :offset-date-time
      (when x
        (offset-date-time/plus-months
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-weeks
     (binary-op/make-binary-op
      :offset-date-time-add-day :offset-date-time
      (when x
        (offset-date-time/plus-weeks
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-days
     (binary-op/make-binary-op
      :offset-date-time-add-day :offset-date-time
      (when x
        (offset-date-time/plus-seconds
         x
         (long (Math/round
                (* (if y (double y) 0.0)
                   (dtype-dt/seconds-in-day)))))))
     :add-hours
     (binary-op/make-binary-op
      :offset-date-time-add-hour :offset-date-time
      (when x
        (offset-date-time/plus-seconds
         x
         (long (Math/round
                (* (if y (double y) 0.0)
                   (dtype-dt/seconds-in-hour)))))))
     :add-minutes
     (binary-op/make-binary-op
      :offset-date-time-add-minute :offset-date-time
      (when x
        (offset-date-time/plus-seconds
         x
         (long (Math/round
                (* (if y (double y) 0.0)
                   (dtype-dt/seconds-in-minute)))))))
     :add-seconds
     (binary-op/make-binary-op
      :offset-date-time-add-second :offset-date-time
      (when x
        (offset-date-time/plus-seconds
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-milliseconds
     (binary-op/make-binary-op
      :offset-date-time-add-millisecond :offset-date-time
      (when x
        (offset-date-time/plus-nanos
         x
         (* (dtype-dt/nanoseconds-in-millisecond)
            (long (Math/round
                   (if y (double y) 0.0)))))))}
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
   {:numeric-ops
    {:add-years
     (binary-op/make-binary-op
      :local-date-time-add-day :local-date-time
      (when x
        (local-date-time/plus-years
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-months
     (binary-op/make-binary-op
      :local-date-time-add-day :local-date-time
      (when x
        (local-date-time/plus-months
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-weeks
     (binary-op/make-binary-op
      :local-date-time-add-day :local-date-time
      (when x
        (local-date-time/plus-weeks
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-days
     (binary-op/make-binary-op
      :local-date-time-add-day :local-date-time
      (when x
        (local-date-time/plus-seconds
         x
         (long (Math/round
                (* (if y (double y) 0.0)
                   (dtype-dt/seconds-in-day)))))))
     :add-hours
     (binary-op/make-binary-op
      :local-date-time-add-hour :local-date-time
      (when x
        (local-date-time/plus-seconds
         x
         (long (Math/round
                (* (if y (double y) 0.0)
                   (dtype-dt/seconds-in-hour)))))))
     :add-minutes
     (binary-op/make-binary-op
      :local-date-time-add-minute :local-date-time
      (when x
        (local-date-time/plus-seconds
         x
         (long (Math/round
                (* (if y (double y) 0.0)
                   (dtype-dt/seconds-in-minute)))))))
     :add-seconds
     (binary-op/make-binary-op
      :local-date-time-add-second :local-date-time
      (when x
        (local-date-time/plus-seconds
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-milliseconds
     (binary-op/make-binary-op
      :local-date-time-add-millisecond :local-date-time
      (when x
        (local-date-time/plus-nanos
         x
         (* (dtype-dt/nanoseconds-in-millisecond)
            (long (Math/round
                   (if y (double y) 0.0)))))))}
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
   {:numeric-ops
    {:add-years
     (binary-op/make-binary-op
      :packed-local-date-time-add-day :packed-local-date-time
      (packed-local-date-time/plus-years x y))
     :add-months
     (binary-op/make-binary-op
      :packed-local-date-time-add-day :packed-local-date-time
      (packed-local-date-time/plus-months x y))
     :add-weeks
     (binary-op/make-binary-op
      :packed-local-date-time-add-day :packed-local-date-time
      (packed-local-date-time/plus-weeks x y))
     :add-days
     (binary-op/make-binary-op
      :packed-local-date-time-add-day :packed-local-date-time
      (packed-local-date-time/plus-seconds x y))
     :add-hours
     (binary-op/make-binary-op
      :packed-local-date-time-add-hour :packed-local-date-time
      (packed-local-date-time/plus-seconds x y))
     :add-minutes
     (binary-op/make-binary-op
      :packed-local-date-time-add-minute :packed-local-date-time
      (packed-local-date-time/plus-seconds x y))
     :add-seconds
     (binary-op/make-binary-op
      :packed-local-date-time-add-second :packed-local-date-time
      (packed-local-date-time/plus-seconds x y))
     :add-milliseconds
     (binary-op/make-binary-op
      :packed-local-date-time-add-millisecond :packed-local-date-time
      (packed-local-date-time/plus-nanos
       x
       (* (dtype-dt/nanoseconds-in-millisecond)
          y)))}
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
   {:numeric-ops
    {:add-years
     (binary-op/make-binary-op
      :local-date-add-day :local-date
      (when x
        (local-date/plus-years
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-months
     (binary-op/make-binary-op
      :local-date-add-day :local-date
      (when x
        (local-date/plus-months
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-weeks
     (binary-op/make-binary-op
      :local-date-add-day :local-date
      (when x
        (local-date/plus-weeks
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-days
     (binary-op/make-binary-op
      :local-date-add-day :local-date
      (when x
        (local-date/plus-days
         x
         (long (Math/round
                (if y (double y) 0.0))))))}
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
   {:numeric-ops
    {:add-years
     (binary-op/make-binary-op
      :packed-local-date-add-day :packed-local-date
      (int (packed-local-date/plus-years
            x
            (long (Math/round
                   (if y (double y) 0.0))))))
     :add-months
     (binary-op/make-binary-op
      :packed-local-date-add-day :packed-local-date
      (int (packed-local-date/plus-months
            x
            (long (Math/round
                   (if y (double y) 0.0))))))
     :add-weeks
     (binary-op/make-binary-op
      :packed-local-date-add-day :packed-local-date
      (int (packed-local-date/plus-weeks
            x
            (long (Math/round
                   (if y (double y) 0.0))))))
     :add-days
     (binary-op/make-binary-op
      :packed-local-date-add-day :packed-local-date
      (int (packed-local-date/plus-days
            x
            (long (Math/round
                   (if y (double y) 0.0))))))}
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
   {:numeric-ops
    {:add-hours
     (binary-op/make-binary-op
      :local-time-add-hour :local-time
      (when x
        (local-time/plus-seconds
         x
         (long (Math/round
                (* (if y (double y) 0.0)
                   (dtype-dt/seconds-in-hour)))))))
     :add-minutes
     (binary-op/make-binary-op
      :local-time-add-minute :local-time
      (when x
        (local-time/plus-seconds
         x
         (long (Math/round
                (* (if y (double y) 0.0)
                   (dtype-dt/seconds-in-minute)))))))
     :add-seconds
     (binary-op/make-binary-op
      :local-time-add-second :local-time
      (when x
        (local-time/plus-seconds
         x
         (long (Math/round
                (if y (double y) 0.0))))))
     :add-milliseconds
     (binary-op/make-binary-op
      :local-time-add-millisecond :local-time
      (when x
        (local-time/plus-nanos
         x
         (* (dtype-dt/nanoseconds-in-millisecond)
            (long (Math/round
                   (if y (double y) 0.0)))))))}
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
   {:numeric-ops
    {:add-hours
     (binary-op/make-binary-op
      :packed-local-time-add-hour :packed-local-time
      (int (packed-local-time/plus-seconds
            x (* y  (dtype-dt/seconds-in-hour)))))
     :add-minutes
     (binary-op/make-binary-op
      :packed-local-time-add-minute :packed-local-time
      (int (packed-local-time/plus-seconds
            x (* y (dtype-dt/seconds-in-minute)))))
     :add-seconds
     (binary-op/make-binary-op
      :packed-local-time-add-second :packed-local-time
      (int (packed-local-time/plus-seconds x y)))
     :add-milliseconds
     (binary-op/make-binary-op
      :packed-local-time-add-millisecond :packed-local-time
      (int (packed-local-time/plus-millis x y)))}
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


(def date-datatypes #{:instant :local-date :local-date-time :local-time
                      :packed-instant :packed-local-date :packed-local-date-time
                      :packed-local-time :zoned-date-time :offset-date-time})

(defn- date-datatype?
  [dtype]
  (boolean
   (date-datatypes dtype)))


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


(defn add-years
  [lhs rhs]
  (perform-commutative-numeric-op lhs rhs :add-years))


(defn add-months
  [lhs rhs]
  (perform-commutative-numeric-op lhs rhs :add-months))


(defn add-weeks
  [lhs rhs]
  (perform-commutative-numeric-op lhs rhs :add-weeks))


(defn add-days
  [lhs rhs]
  (perform-commutative-numeric-op lhs rhs :add-days))


(defn add-hours
  [lhs rhs]
  (perform-commutative-numeric-op lhs rhs :add-hours))


(defn add-minutes
  [lhs rhs]
  (perform-commutative-numeric-op lhs rhs :add-minutes))


(defn add-seconds
  [lhs rhs]
  (perform-commutative-numeric-op lhs rhs :add-seconds))


(defn add-milliseconds
  [lhs rhs]
  (perform-commutative-numeric-op lhs rhs :add-milliseconds))


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


(defn min
  [lhs rhs]
  (perform-binary-op lhs rhs :min))


(defn max
  [lhs rhs]
  (perform-binary-op lhs rhs :max))


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


(defn reduce-min
  [lhs]
  (perform-commutative-binary-reduction lhs :min))


(defn reduce-max
  [lhs]
  (perform-commutative-binary-reduction lhs :max))


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
