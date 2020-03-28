(ns tech.v2.datatype.datetime.packed-local-date-time
  (:import [tech.v2.datatype PackedLocalDateTime]
           [java.time.temporal TemporalUnit ChronoUnit]))


(defn plus-years
  ^long [^long x ^long y]
  (PackedLocalDateTime/plus x y ChronoUnit/YEARS))

(defn plus-months
  ^long [^long x ^long y]
  (PackedLocalDateTime/plus x y ChronoUnit/MONTHS))

(defn plus-weeks
  ^long [^long x ^long y]
  (PackedLocalDateTime/plus x y ChronoUnit/WEEKS))

(defn plus-seconds
  ^long [^long x ^long y]
  (PackedLocalDateTime/plus x y ChronoUnit/SECONDS))

(defn plus-nanos
  ^long [^long x ^long y]
  (PackedLocalDateTime/plus x y ChronoUnit/NANOS))
