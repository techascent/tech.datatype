(ns tech.v2.datatype.datetime.packed-local-date
  (:require [tech.v2.datatype.datetime :as dtype-dt])
  (:import [tech.v2.datatype PackedLocalDate]
           [java.time.temporal TemporalUnit ChronoUnit]))


(defn plus-years
  ^long [^long x ^long y]
  (PackedLocalDate/plus y ChronoUnit/YEARS x))

(defn plus-months
  ^long [^long x ^long y]
  (PackedLocalDate/plus y ChronoUnit/MONTHS x))

(defn plus-weeks
  ^long [^long x ^long y]
  (PackedLocalDate/plus y ChronoUnit/WEEKS x))

(defn plus-days
  ^long [^long x ^long y]
  (PackedLocalDate/plus y ChronoUnit/DAYS x))

(defn plus-seconds
  ^long [^long x ^long y]
  (PackedLocalDate/plus (quot y
                              (dtype-dt/seconds-in-day))
                        ChronoUnit/DAYS x))
