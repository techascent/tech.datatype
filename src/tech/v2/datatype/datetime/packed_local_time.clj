(ns tech.v2.datatype.datetime.packed-local-time
  (:import [tech.v2.datatype PackedLocalTime]
           [java.time.temporal TemporalUnit ChronoUnit]))


(defn plus-seconds
  ^long [^long packed-time ^long secs]
  (PackedLocalTime/plusSeconds secs packed-time))


(defn plus-millis
  ^long [^long packed-time ^long secs]
  (PackedLocalTime/plusMilliseconds secs packed-time))
