(ns tech.v2.datatype.datetime
  "Conversion routines and minimal support for date-time and packed date-time."
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.object-datatypes :as dtype-obj]
            [primitive-math :as pmath])
  (:import [java.time ZoneId ZoneOffset
            Instant ZonedDateTime OffsetDateTime
            LocalDate LocalDateTime LocalTime
            OffsetTime]
           [java.util Date]
   [tech.v2.datatype
            PackedInstant PackedLocalDate
            PackedLocalTime PackedLocalDateTime
            ObjectReader LongReader IntReader]))


(set! *warn-on-reflection* true)


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
  (^Instant [milliseconds]
   (Instant/ofEpochMilli (long milliseconds)))
  (^Instant []
   (Instant/now)))


(defn seconds-since-epoch->instant
  ^Instant [seconds]
  (milliseconds-since-epoch->instant (* (long seconds) 1000)))


(defn instant
  (^Instant []
   (Instant/now))
  (^Instant [epoch-millis]
   (milliseconds-since-epoch->instant
    epoch-millis)))


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


(defn zoned-date-time
  ^ZonedDateTime []
  (ZonedDateTime/now))


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


(defn offset-date-time
  ^OffsetDateTime []
  (OffsetDateTime/now))



(defn instant->local-date-time
  (^LocalDateTime [^Instant inst ^ZoneId zone-id]
   (LocalDateTime/ofInstant inst zone-id))
  (^LocalDateTime [^Instant inst]
   (LocalDateTime/ofInstant inst (ZoneId/systemDefault))))


(defn local-date-time->instant
  (^Instant [^LocalDateTime ldt ^ZoneOffset offset]
   (.toInstant ldt offset))
  (^Instant [^LocalDateTime ldt]
   (local-date-time->instant (system-zone-offset))))


(defn local-date-time
  ([]
   (LocalDateTime/now))
  ([milliseconds-since-epoch]
   (-> (instant milliseconds-since-epoch)
       (instant->local-date-time))))


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
  (^LocalTime [millis-since-epoch]
   (-> (local-date-time millis-since-epoch)
       (local-date-time->local-time))))


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


(defn local-date
  (^LocalDate []
   (LocalDate/now))
  (^LocalDate [millis-since-epoch]
   (-> (local-date-time millis-since-epoch)
       (local-date-time->local-date))))


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


(dtype-obj/add-object-datatype Instant :instant instant)
(dtype-obj/add-object-datatype LocalDate :local-date local-date)
(dtype-obj/add-object-datatype LocalDateTime :local-date-time local-date-time)
(dtype-obj/add-object-datatype LocalTime :local-time local-time)


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
    :local-date-time `(PackedLocalDateTime/asLocalDateTime ~item)))


(defn instant->packed-instant
  ^long [^Instant inst]
  (compile-time-pack inst :instant))


(defn packed-instant
  ^Instant [^long inst]
  (compile-time-unpack inst :instant))


(defn local-date->packed-local-date
  "This long can be represented by an integer safely with unchecked-int."
  ^long [^LocalDate ld]
  (compile-time-pack ld :local-date))


(defn packed-local-date->local-date
  ^LocalDate [^long pld]
  (compile-time-unpack pld :local-date))


(defn local-time->packed-local-time
  ^long [^LocalTime lt]
  (compile-time-pack lt :local-time))


(defn packed-local-time->local-time
  ^LocalTime [^PackedLocalTime plt]
  (compile-time-unpack plt :local-time))


(defn local-date-time->packed-local-date-time
  ^long [^LocalDateTime ld]
  (compile-time-pack ld :local-date-time))


(defn packed-local-date-time->local-date-time
  ^LocalDateTime [^long pld]
  (compile-time-unpack pld :local-date-time))


(def packable-datatypes
  [:local-date :local-time :instant :local-date-time])


(def packable-datatype->primitive-datatype
  {:local-date :int32
   :local-time :int32
   :local-date-time :int64
   :instant :int64})


(defn instant-reader->packed-instant-reader
  ^LongReader [reader]
  (let [reader (typecast/datatype->reader :object reader)]
    (reify LongReader
      (lsize [rdr] (.lsize reader))
      (read [rdr idx] (-> (.read reader idx)
                          (compile-time-pack :instant))))))


(defn packed-instant-reader->instant-reader
  ^ObjectReader [reader]
  (let [reader (typecast/datatype->reader :int64 reader)]
    (reify ObjectReader
      (getDatatype [rdr] :instant)
      (lsize [rdr] (.lsize reader))
      (read [rdr idx] (-> (.read reader idx)
                          (compile-time-unpack :instant))))))


(defn local-date-time-reader->packed-local-date-time-reader
  ^LongReader [reader]
  (let [reader (typecast/datatype->reader :object reader)]
    (reify LongReader
      (lsize [rdr] (.lsize reader))
      (read [rdr idx] (-> (.read reader idx)
                          (compile-time-pack :local-date-time))))))



(defn packed-local-date-time-reader->local-date-time-reader
  ^ObjectReader [reader]
  (let [reader (typecast/datatype->reader :int64 reader)]
    (reify ObjectReader
      (getDatatype [rdr] :local-date-time)
      (lsize [rdr] (.lsize reader))
      (read [rdr idx] (-> (.read reader idx)
                          (compile-time-unpack :local-date-time))))))


(defn local-date-reader->packed-local-date-reader
  ^IntReader [reader]
  (let [reader (typecast/datatype->reader :object reader)]
    (reify IntReader
      (lsize [rdr] (.lsize reader))
      (read [rdr idx] (-> (.read reader idx)
                          (compile-time-pack :local-date))))))



(defn packed-local-date-reader->local-date-reader
  ^ObjectReader [reader]
  (let [reader (typecast/datatype->reader :int32 reader)]
    (reify ObjectReader
      (getDatatype [rdr] :local-date)
      (lsize [rdr] (.lsize reader))
      (read [rdr idx] (-> (.read reader idx)
                          (compile-time-unpack :local-date))))))


(defn local-time-reader->packed-local-time-reader
  ^IntReader [reader]
  (let [reader (typecast/datatype->reader :object reader)]
    (reify IntReader
      (lsize [rdr] (.lsize reader))
      (read [rdr idx] (-> (.read reader idx)
                          (compile-time-pack :local-time))))))



(defn packed-local-time-reader->local-time-reader
  ^ObjectReader [reader]
  (let [reader (typecast/datatype->reader :int32 reader)]
    (reify ObjectReader
      (getDatatype [rdr] :local-time)
      (lsize [rdr] (.lsize reader))
      (read [rdr idx] (-> (.read reader idx)
                          (compile-time-unpack :local-time))))))
