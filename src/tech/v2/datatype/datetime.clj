(ns tech.v2.datatype.datetime
  "Conversion routines and minimal support for date-time and packed date-time."
  (:import [java.time LocalDateTime ZonedDateTime
            Instant OffsetDateTime ZoneId ZoneOffset]
           [java.util Date]))


(set! *warn-on-reflection* true)


(defn seconds-since-epoch->java-date
  ^Date [seconds]
  (-> seconds
      long
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


(defn instant
  ^Instant []
  (Instant/now))


(defn seconds-since-epoch->instant
  (^Instant [seconds]
   (milliseconds-since-epoch->instant (* (long seconds) 1000)))
  (^Instant []
   (Instant/now)))


(defn instant->milliseconds-since-epoch
  (^long [^Instant instant]
   (.toEpochMilli instant))
  (^long []
   (java-date->milliseconds-since-epoch)))


(defn zoned-date-time
  ^ZonedDateTime []
  (ZonedDateTime/now))


(defn instant->zoned-date-time
  (^ZonedDateTime [^Instant inst ^ZoneId zid]
   (.atZone inst zid))
  (^ZonedDateTime [^Instant inst]
   (instant->zoned-date-time inst (ZoneId/systemDefault)))
  (^ZonedDateTime []
   (instant->zoned-date-time (Instant/now))))


(defn zoned-date-time->instant
  ^Instant [^ZonedDateTime zid]
  (.toInstant zid))


(defn instant->offset-date-time
  (^OffsetDateTime [^Instant inst ^ZoneOffset offset]
   (.atOffset inst offset))
  (^OffsetDateTime [^Instant inst]
   (.atOffset inst (.. (OffsetDateTime/now) getOffset)))
  (^OffsetDateTime []
   (OffsetDateTime/now)))


(defn offset-date-time->instant
  ^Instant [^OffsetDateTime of]
  (.toInstant of))


(defn offset-date-time
  ^OffsetDateTime []
  (OffsetDateTime/now))
