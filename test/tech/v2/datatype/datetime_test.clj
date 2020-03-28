(ns tech.v2.datatype.datetime-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
            [clojure.test :refer [deftest is]]))


(deftest instant-add-day
  (let [base-instant (dtype-dt/instant)
        scalar-res (dtype-dt-ops/add-days base-instant 1)
        iterable-res (dtype-dt-ops/add-days base-instant
                                            (apply list (range 5)))
        reader-res (dtype-dt-ops/add-days base-instant
                                          (range 5))]
    (is (= [:instant :instant :instant :instant]
           (mapv dtype/get-datatype [base-instant
                                     scalar-res
                                     iterable-res
                                     reader-res])))))
