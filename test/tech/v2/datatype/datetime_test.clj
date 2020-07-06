(ns tech.v2.datatype.datetime-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.tensor :as dtt]
            [clojure.test :refer [deftest is]])
  (:import [java.time.temporal ChronoUnit]))


(deftest instant
  (let [base-instant (dtype-dt/instant)
        scalar-res (dtype-dt-ops/plus-days base-instant 1)
        iterable-res (dtype-dt-ops/plus-days base-instant
                                            (apply list (range 5)))
        reader-res (dtype-dt-ops/plus-days base-instant
                                           (range 5))]
    (is (= [:instant :instant :instant :instant]
           (mapv dtype/get-datatype [base-instant
                                     scalar-res
                                     iterable-res
                                     reader-res])))
    (is (dfn/reduce-and (dtype-dt-ops/== iterable-res reader-res)))
    (is (every? #(< (long %) 100)
                (dtype-dt-ops/difference-milliseconds
                 iterable-res
                 (->> (dtype-dt/pack iterable-res)
                      (dtype-dt/unpack)))))
    (is (= :packed-instant (dtype/get-datatype (dtype-dt/pack iterable-res))))
    (is (every? #(< (long %) 100)
                (dtype-dt-ops/difference-milliseconds
                 iterable-res
                 ;;test math on packed types
                 (->> (repeat 5 base-instant)
                      (dtype-dt/pack)
                      (dtype-dt-ops/plus-days (range 5))
                      (dtype-dt/unpack))))
        (format "expected:%s\n     got:%s"
                (mapv #(.toString ^Object %)
                      iterable-res)
                (mapv #(.toString ^Object %)
                      (->> (repeat 5 base-instant)
                           (dtype-dt/pack)
                           (dtype-dt-ops/plus-days (range 5))
                           (dtype-dt/unpack)))))))


(deftest local-date-time-add-day
  (let [base-elem (dtype-dt/local-date-time)
        scalar-res (dtype-dt-ops/plus-days base-elem 1)
        iterable-res (dtype-dt-ops/plus-days base-elem
                                            (apply list (range 5)))
        reader-res (dtype-dt-ops/plus-days base-elem
                                           (range 5))]
    (is (= [:local-date-time :local-date-time :local-date-time :local-date-time]
           (mapv dtype/get-datatype [base-elem
                                     scalar-res
                                     iterable-res
                                     reader-res])))
    (is (dfn/reduce-and (dtype-dt-ops/== iterable-res reader-res)))
    (is (every? #(< (long %) 100)
                (dtype-dt-ops/difference-milliseconds iterable-res
                                                      (->> (dtype-dt/pack iterable-res)
                                                           (dtype-dt/unpack)))))
    (is (= :packed-local-date-time (dtype/get-datatype (dtype-dt/pack iterable-res))))
    (is (every? #(< (long %) 100)
                (dtype-dt-ops/difference-milliseconds
                 iterable-res
                 (->> (repeat 5 base-elem)
                      (dtype-dt/pack)
                      (dtype-dt-ops/plus-days (range 5))
                      (dtype-dt/unpack))))
        (format "expected:%s\n     got:%s"
                (mapv #(.toString ^Object %)
                      iterable-res)
                (mapv #(.toString ^Object %)
                      (->> (repeat 5 base-elem)
                           (dtype-dt/pack)
                           (dtype-dt-ops/plus-days (range 5))
                           (dtype-dt/unpack)))))))


(deftest epoch-seconds-and-millis-have-correct-datatype
  (let [item-seq (repeat 5 (dtype-dt/instant))]
    (is (= :epoch-milliseconds
           (-> (dtype-dt-ops/get-epoch-milliseconds item-seq)
               (dtype/get-datatype))))
    (is (= :epoch-seconds
           (-> (dtype-dt-ops/get-epoch-seconds item-seq)
               (dtype/get-datatype))))))


(deftest epoch-seconds-and-millis-work-on-local-date-time-and-local-date
  (let [item-seq (repeat 5 (dtype-dt/local-date-time))
        packed-seq (dtype-dt/pack item-seq)]
    (is (= :epoch-milliseconds
           (-> (dtype-dt-ops/get-epoch-milliseconds item-seq)
               (dtype/get-datatype))))
    (is (= :epoch-seconds
           (-> (dtype-dt-ops/get-epoch-seconds item-seq)
               (dtype/get-datatype))))
    (is (every? number? (dtype-dt-ops/get-epoch-milliseconds item-seq)))
    (is (= :epoch-milliseconds
           (-> (dtype-dt-ops/get-epoch-milliseconds packed-seq)
               (dtype/get-datatype))))
    (is (every? number? (dtype-dt-ops/get-epoch-milliseconds packed-seq)))
    (is (= :epoch-seconds
           (-> (dtype-dt-ops/get-epoch-seconds packed-seq)
               (dtype/get-datatype)))))
  (let [item-seq (repeat 5 (dtype-dt/local-date))
        packed-seq (dtype-dt/pack item-seq)]
    (is (= :epoch-milliseconds
           (-> (dtype-dt-ops/get-epoch-milliseconds item-seq)
               (dtype/get-datatype))))
    (is (= :epoch-seconds
           (-> (dtype-dt-ops/get-epoch-seconds item-seq)
               (dtype/get-datatype))))
    (is (every? number? (dtype-dt-ops/get-epoch-seconds item-seq)))
    (is (= :epoch-milliseconds
           (-> (dtype-dt-ops/get-epoch-milliseconds packed-seq)
               (dtype/get-datatype))))
    (is (= :epoch-seconds
           (-> (dtype-dt-ops/get-epoch-seconds packed-seq)
               (dtype/get-datatype))))
    (is (every? number? (dtype-dt-ops/get-epoch-seconds packed-seq)))))


(deftest epoch-times
  (let [start-zdt (dtype-dt/milliseconds-since-epoch->zoned-date-time 0)
        now-zdt (dtype-dt/zoned-date-time)]
    (is (= (.between (ChronoUnit/MINUTES) start-zdt now-zdt)
           (dtype-dt-ops/get-epoch-minutes now-zdt)))
    (is (= (.between (ChronoUnit/HOURS) start-zdt now-zdt)
           (dtype-dt-ops/get-epoch-hours now-zdt)))
    (is (= (.between (ChronoUnit/DAYS) start-zdt now-zdt)
           (dtype-dt-ops/get-epoch-days now-zdt)))
    (is (= (.between (ChronoUnit/WEEKS) start-zdt now-zdt)
           (dtype-dt-ops/get-epoch-weeks now-zdt)))))



;;I really don't expect tensors to be used with date time objects but regardless
;;the basic expectations of these things (like functions that take a tensor return
;;a tensor) all need to be respected.
(deftest simple-tensor-ops
  (let [src-data (dtype/make-container
                  :typed-buffer
                  :instant
                  9)
        test-tens (dtt/reshape src-data [3 3])]
    (is (= [3 3] (dtype/shape test-tens)))
    ;;Also, it has to print without exception
    (is (string? (.toString test-tens)))
    (let [epoch-tens (dtype-dt-ops/get-epoch-milliseconds test-tens)]
      (is (= [3 3] (dtype/shape epoch-tens))))))


(deftest durations
  (let [src-data (dtype/make-container
                  :typed-buffer
                  :duration
                  5)
        hours-up (dtype-dt-ops/plus-hours src-data (range 5))
        zoned-dt (dtype/make-container
                  :typed-buffer
                  :zoned-date-time
                  5)
        updated-dt (dtype-dt-ops/plus-duration zoned-dt hours-up)
        packed-src (dtype-dt/pack src-data)
        packed-hours-up (dtype-dt-ops/plus-hours packed-src (range 5))
        updated-dt-2 (dtype-dt-ops/plus-duration zoned-dt packed-hours-up)
        double-packed (dtype-dt-ops/plus-duration packed-hours-up packed-hours-up)
        updated-dt-3 (dtype-dt-ops/plus-duration zoned-dt double-packed)]
    (is (every? #(.equals ^Object (first %) (second %))
                (map vector updated-dt updated-dt-2)))
    (is (every? #(not (.equals ^Object (first %) (second %)))
                (map vector (rest updated-dt) (rest updated-dt-3))))))



(deftest descriptive-stats-instant
  (let [{:keys [min max mean]}
        (-> (dtype-dt/instant)
            (dtype-dt-ops/plus-days (range 5))
            (dtype-dt/pack)
            (dtype-dt-ops/millisecond-descriptive-stats))]
    (is (and (dtype-dt-ops/< min mean)
             (dtype-dt-ops/< mean max)))
    (is (every? dtype-dt/datetime-datatype?
                (map dtype/get-datatype
                     [min mean max])))))


(deftest descriptive-stats-zoned-date-time
  (let [{:keys [min max mean]}
        (-> (dtype-dt/zoned-date-time)
            (dtype-dt-ops/plus-days (range 5))
            (dtype-dt-ops/millisecond-descriptive-stats))]
    (is (and (dtype-dt-ops/< min mean)
             (dtype-dt-ops/< mean max)))
    (is (every? dtype-dt/datetime-datatype?
                (map dtype/get-datatype
                     [min mean max])))))


(deftest descriptive-stats-duration
  (let [{:keys [min max mean standard-deviation]}
        (-> (dtype-dt/duration)
            (dtype-dt-ops/plus-days (range 5))
            (dtype-dt/pack)
            (dtype-dt-ops/millisecond-descriptive-stats))]
    (is (and (dtype-dt-ops/< min mean)
             (dtype-dt-ops/< mean max)))
    (is (every? dtype-dt/datetime-datatype?
                (map dtype/get-datatype
                     [min mean max standard-deviation])))))


(deftest argsort-instants
  (let [inst-data (-> (dtype/make-container :typed-buffer :instant 20)
                      (dtype-dt-ops/plus-days (range 20)))
        sorted-data (dfn/argsort inst-data)]
    (is (apply < sorted-data))))


(deftest duration-days-are-doubles
  (let [hour-range (range 0 96 12)
        test-data (dtype-dt-ops/plus-hours (dtype-dt/duration) hour-range)]
    (is (= (vec (dtype-dt-ops/get-days test-data))
           (vec (map #(/ % 24.0) hour-range))))))


(deftest add-duration-days-to-local-dates
  (let [day-range (range 5)
        test-duration (dtype-dt/duration)
        test-local-date (dtype-dt/local-date)
        test-data (dtype-dt/pack (dtype-dt-ops/plus-days
                                  test-duration day-range))
        date-data (dtype-dt/pack (dtype-dt-ops/plus-duration
                                  test-local-date test-data))
        expected (dtype-dt-ops/plus-days test-local-date day-range)]
    (is (= (vec (dfn/- (dtype-dt-ops/get-days date-data)
                       (dtype-dt-ops/get-days test-local-date)))
           (vec (dfn/- (dtype-dt-ops/get-days expected)
                       (dtype-dt-ops/get-days test-local-date))))
        (pr-str {:test-duration (str test-duration)
                 :test-local-date (str test-local-date)}))))



(deftest add-duration-days-to-instants
  (let [hour-range (range 0 96 12)
        test-duration (dtype-dt/duration)
        test-instant (dtype-dt/instant)
        test-data (dtype-dt/pack (dtype-dt-ops/plus-hours
                                  test-duration hour-range))
        instant-data (dtype-dt/pack (dtype-dt-ops/plus-duration
                                     test-instant test-data))]
    (is (dfn/equals
         (vec (dfn//
               (dfn/- (dtype-dt-ops/get-epoch-milliseconds instant-data)
                      (dtype-dt-ops/get-epoch-milliseconds test-instant))
               (double (dtype-dt/milliseconds-in-day))))
         (vec (map #(/ % 24.0) hour-range)))
        (pr-str {:test-duration (str test-duration)
                 :test-local-date (str test-instant)}))))


(deftest packed-sub-buffer
  (let [packed-data (dtype-dt/pack (dtype-dt/local-date))
        packed-reader (dtype/make-container :list :packed-local-date
                                            (repeat 10 packed-data))
        data (dtype/sub-buffer (dtype/->reader packed-reader) 0 3)]
    (is (= :packed-local-date (dtype/get-datatype packed-reader)))
    (is (= :packed-local-date (dtype/get-datatype (dtype/->reader packed-reader))))
    (is (= :packed-local-date (dtype/get-datatype data)))))


(deftest vectorized-epoch-second-conversions
  (let [src-data (into-array
                  (repeat 4 (dtype-dt/local-date)))
        millis (dtype-dt-ops/local-date->milliseconds-since-epoch
                src-data 0 (dtype-dt/system-zone-id))]
    (is (= (vec src-data)
           (vec (dtype-dt-ops/milliseconds-since-epoch->local-date
                 millis (dtype-dt/system-zone-id))))))
  (let [src-data (into-array (repeat 4 (dtype-dt/local-date-time)))
        millis (dtype-dt-ops/local-date-time->milliseconds-since-epoch
                src-data (dtype-dt/system-zone-id))]
    (is (= (vec src-data)
           (vec (dtype-dt-ops/milliseconds-since-epoch->local-date-time
                 millis (dtype-dt/system-zone-id)))))

    ;;Fails if the host machine timezone is UTC :-).  Works otherwise.
    #_(is (not= (vec src-data)
                (vec (dtype-dt-ops/milliseconds-since-epoch->local-date-time
                      millis (dtype-dt/utc-zone-id)))))
    ))


(deftest pack-unpack-work-on-anything
  (is (= (vec (long-array [1 2 3 4]))
         (vec (dtype-dt/pack (long-array [1 2 3 4])))))
  (is (= (vec (dtype-dt/pack (into-array (repeatedly 5 #(dtype-dt/local-date)))))
         (vec (dtype-dt/pack (dtype-dt/pack
                              (into-array
                               (repeatedly 5 #(dtype-dt/local-date)))))))))


(deftest pack-unpack-work-on-anything
  (is (= (vec (long-array [1 2 3 4]))
         (vec (dtype-dt/pack (long-array [1 2 3 4])))))
  (is (= (vec (dtype-dt/pack (into-array (repeatedly 5 #(dtype-dt/local-date)))))
         (vec (dtype-dt/pack (dtype-dt/pack
                              (into-array
                               (repeatedly 5 #(dtype-dt/local-date))))))))
  (is (= (vec (long-array [1 2 3 4]))
         (vec (dtype-dt/unpack (long-array [1 2 3 4])))))
  (is (= (vec (dtype-dt/unpack (into-array (repeatedly 5 #(dtype-dt/local-date)))))
         (vec (dtype-dt/unpack (dtype-dt/pack
                                (into-array
                                 (repeatedly 5 #(dtype-dt/local-date)))))))))


(deftest to-milliseconds-for-packed-local-dt
  (is (= 1577877753000
         (dtype-dt/to-milliseconds 568580556948144360 :packed-local-date-time))))

(deftest to-milliseconds-test
  (doseq [dtype dtype-dt/datetime-datatypes]
    (let [data
          (dtype-dt/from-milliseconds 0 dtype)]
      (is (= 0 (dtype-dt/to-milliseconds data dtype))))))
