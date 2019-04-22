(ns tech.v2.datatype.cheatsheet-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.tensor :as tens]
            [tech.v2.datatype.functional :as dtype-fn]
            [clojure.test :refer :all]))



(deftest cheatsheet-test
  (let [test-data (dtype-fn/+ (range 10 0 -1) 5)
        indexes (dtype-fn/argsort test-data)]
    (is (= [6.0 7.0 8.0 9.0 10.0 11.0 12.0 13.0 14.0 15.0]
           (dtype-fn/indexed-reader indexes (vec test-data)))))

  (let [test-tens (tens/->tensor (partition 3 (range 9)))
        ;;This actually tests quite a lot but type promotion is one
        ;;thing.
        result-tens (dtype-fn/+ 2 (tens/select test-tens [1 0] :all))]
    (is (= [[5.000 6.000 7.000]
            [2.000 3.000 4.000]]
           (tens/->jvm result-tens)))))
