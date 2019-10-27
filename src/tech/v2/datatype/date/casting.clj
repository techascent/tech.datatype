(ns tech.v2.datatype.date.casting
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.protocols :as dtype-proto])
  (:import [tech.v2.datatype DateUtility
            LongReader
            LongWriter
            ObjectReader]
           [java.time Instant LocalDateTime ZonedDateTime ZoneId]
           [java.util TimeZone]))


(set! *warn-on-reflection* true)


(defn options->time-zone
  ^TimeZone [options]
  (let [time-zone (:time-zone options)]
    (cond
      (instance? TimeZone time-zone)
      time-zone
      (string? time-zone)
      (TimeZone/getTimeZone (str time-zone))
      :else
      (TimeZone/getDefault))))


(defmulti datetime-reader-cast
  "Family of cast functions for going to/from/between datetime family objects"
  (fn [src-reader src-datatype dst-datatype options]
    [(casting/datetime-family-class src-datatype)
     (casting/datetime-family-class dst-datatype)]))

(defmethod datetime-reader-cast [:out-of-family :out-of-family]
  [src-reader src-datatype dst-datatype options]
  (dtype-proto/->reader src-reader (assoc options :datatype dst-datatype)))

;;Going into the datetime family we assume src is in milliseconds-since-epoch
(defmethod datetime-reader-cast [:out-of-family :object]
  [src-reader src-datatype dst-datatype options]
  (datetime-reader-cast src-reader (casting/datetime-datatype) dst-datatype options))

;;Out of family to concrete means we reinterpret the values as whatever concrete
;;datetime type you want.
(defmethod datetime-reader-cast [:out-of-family :concrete]
  [src-reader src-datatype dst-datatype options]
  (let [unchecked? (:unchecked? options)
        src-reader (typecast/datatype->reader :int64 src-reader unchecked?)]
    (reify LongReader
      (getDatatype [rdr] dst-datatype)
      (lsize [rdr] (.lsize src-reader))
      (read [rdr idx] (.read src-reader idx)))))


(defmethod datetime-reader-cast [:object :concrete]
  [src-reader src-datatype dst-datatype options]
  (let [src-reader (typecast/datatype->reader :object src-reader)
        temp-datatype (casting/datetime-datatype :milliseconds "UTC")]
    (-> (case src-datatype
          :instant
          (reify LongReader
            (getDatatype [rdr] temp-datatype)
            (lsize [rdr] (.lsize src-reader))
            (read [rdr idx]
              (let [^Instant inst (.read src-reader idx)]
                (.toEpochMilli inst))))
          :zoned-date-time
          (reify LongReader
            (getDatatype [rdr] temp-datatype)
            (lsize [rdr] (.lsize src-reader))
            (read [rdr idx]
              (let [^ZonedDateTime dt (.read src-reader idx)]
                (-> (.toInstant dt)
                    (.toEpochMilli)))))
          :local-date-time
          (let [tz (TimeZone/getDefault)]
            (reify LongReader
              (getDatatype [rdr] temp-datatype)
              (lsize [rdr] (.lsize src-reader))
              (read [rdr idx]
                (let [^LocalDateTime dt (.read src-reader idx)]
                  (-> (ZonedDateTime/of dt tz)
                      (.toInstant)
                      (.toEpochMilli)))))))
        (datetime-reader-cast temp-datatype dst-datatype options))))


(defmethod datetime-reader-cast [:concrete :object]
  [src-reader src-datatype dst-datatype options]
  ;;Get source in the form of UTC milliseconds
  (let [src-reader (datetime-reader-cast src-reader src-datatype
                                         (casting/datetime-datatype
                                          :milliseconds "UTC")
                                         options)
        src-reader (typecast/datatype->reader :int64 src-reader)]
    (case dst-datatype
      :instant
      (reify ObjectReader
        (getDatatype [rdr] dst-datatype)
        (lsize [rdr] (.lsize src-reader))
        (read [rdr idx] (Instant/ofEpochMilli (.read src-reader idx))))
      :zoned-date-time
      (let [tz (options->time-zone options)]
        (reify ObjectReader
          (getDatatype [rdr] dst-datatype)
          (lsize [rdr] (.lsize src-reader))
          (read [rdr idx]
            (-> (.read src-reader idx)
                (Instant/ofEpochMilli)
                (ZonedDateTime/ofInstant (.toZoneId tz))))))
      :local-date-time
      (let [tz (options->time-zone options)]
        (reify ObjectReader
          (getDatatype [rdr] dst-datatype)
          (lsize [rdr] (.lsize src-reader))
          (read [rdr idx]
            (-> (.read src-reader idx)
                (Instant/ofEpochMilli)
                (LocalDateTime/ofInstant (.toZoneId tz)))))))))


(defmethod datetime-reader-cast [:concrete :concrete]
  [src-reader src-datatype dst-datatype options]
  (if (= src-datatype dst-datatype)
    src-reader
    (let [src-reader (typecast/datatype->reader :int64 src-reader)
          src-denom (casting/date-denominator (:denominator src-datatype))
          dst-denom (casting/date-denominator (:denominator dst-datatype))
          ^TimeZone src-tz (casting/datetime-datatype->time-zone src-datatype)
          ^TimeZone dst-tz (casting/datetime-datatype->time-zone dst-datatype)
          cur-epoch-millis (long (or (:current-epoch-seconds options)
                                     (-> (Instant/now)
                                         (.toEpochMilli))))
          src-offset (.getOffset src-tz cur-epoch-millis)
          dst-offset (.getOffset dst-tz cur-epoch-millis)
          unchecked? (:unchecked? options)]
      (reify LongReader
        (getDatatype [rdr] dst-datatype)
        (lsize [rdr] (.lsize src-reader))
        (read [rdr idx]
          (let [src-val (- (* src-denom (.read src-reader idx)) src-offset)
                dst-val (+ src-val dst-offset)]
            (when-not (or unchecked?
                          (= 1 dst-denom)
                          (= 0 (rem dst-val dst-denom)))
              (throw (Exception. (format "Src value (%sms) is not commiserate with dst datatype: %s" src-val dst-datatype))))
            (quot dst-val dst-denom)))))))


(defmethod datetime-reader-cast [:object :object]
  [src-reader src-datatype dst-datatype options]
  (if (= src-datatype dst-datatype)
    src-reader
    (let [temp-datatype (casting/datetime-datatype)]
      (-> (datetime-reader-cast src-reader
                                src-datatype
                                temp-datatype
                                options)
          (datetime-reader-cast temp-datatype dst-datatype options)))))
