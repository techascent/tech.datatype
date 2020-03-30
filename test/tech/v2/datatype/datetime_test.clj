(ns tech.v2.datatype.datetime-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
            [tech.v2.datatype.functional :as dfn]
            [clojure.test :refer [deftest is]]
            [clojure.pprint :as pp]))


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


(defn field-compatibility-matrix
  []
  (let [fieldnames (->> dtype-dt/keyword->temporal-field
                        (map first)
                        (concat [:epoch-milliseconds]))]
    (for [[datatype ctor] (sort-by first dtype-dt/datatype->constructor-fn)]
      (->>
       (for [field-name fieldnames]
         [field-name (try
                     (let [accessor (get-in dtype-dt-ops/java-time-ops
                                            [datatype :int64-getters field-name])]
                       (accessor (ctor))
                       true)
                     (catch Throwable e false))])
       (into {})
       (merge {:datatype datatype})))))


(defn print-compatibility-matrix
  ([m]
   (let [field-names (->> (keys (first m))
                          (remove #(= :datatype %))
                          sort)]
     (pp/print-table (concat [:datatype] field-names) m))
   nil))

(defn print-field-compatibility-matrix
  []
  (print-compatibility-matrix
   (field-compatibility-matrix)))


(defn plus-op-compatibility-matrix
  []
  (let [plus-ops (->> dtype-dt/keyword->chrono-unit
                      (map (comp #(keyword (str "plus-" (name %))) first))
                      sort)
        datatypes (sort-by first dtype-dt/datatype->constructor-fn)]
    (for [[datatype ctor] datatypes]
      (->>
       (for [plus-op plus-ops]
         [plus-op (try
                    (let [plus-op (get-in
                                   dtype-dt-ops/java-time-ops
                                   [datatype :numeric-ops plus-op])]
                      (plus-op (ctor) 1)
                      true)
                    (catch Throwable e false))])
       (into {})
       (merge {:datatype datatype})))))


(defn print-plus-op-compatibility-matrix
  []
  (print-compatibility-matrix
   (plus-op-compatibility-matrix)))


(deftest epoch-seconds-and-millis-have-correct-datatype
  (let [item-seq (repeat 5 (dtype-dt/instant))]
    (is (= :epoch-milliseconds
           (-> (dtype-dt-ops/get-epoch-milliseconds item-seq)
               (dtype/get-datatype))))
    (is (= :epoch-seconds
           (-> (dtype-dt-ops/get-epoch-seconds item-seq)
               (dtype/get-datatype))))))
