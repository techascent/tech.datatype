(ns tech.v2.datatype.datetime-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
            [tech.v2.datatype.functional :as dfn]
            [clojure.test :refer [deftest is]]))


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
    (is (dfn/reduce-and
         (dtype-dt-ops/== iterable-res
                          (->> (dtype-dt/pack iterable-res)
                               (dtype-dt/unpack)))))
    (is (= :packed-instant (dtype/get-datatype (dtype-dt/pack iterable-res))))
    (is (dfn/reduce-and
         (dtype-dt-ops/== iterable-res
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
    (is (dfn/reduce-and (dtype-dt-ops/== iterable-res
                                         (->> (dtype-dt/pack iterable-res)
                                              (dtype-dt/unpack)))))
    (is (= :packed-local-date-time (dtype/get-datatype (dtype-dt/pack iterable-res))))
    (is (dfn/reduce-and
         (dtype-dt-ops/== iterable-res
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
