(ns tech.v2.datatype.bitmap-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.bitmap :as bitmap]
            [clojure.test :refer :all]))


(deftest basic-bitmap-test
  (let [ba (bitmap/->bitmap (range 5))
        bb (bitmap/->bitmap (filter even? (range 10)))]
    (is (= (set #{0 4 2}) (set (dtype-proto/set-and ba bb))))
    (is (= (set #{0 1 4 6 3 2 8}) (set (dtype-proto/set-or ba bb))))
    (is (= (set #{7 6 9 5 8}) (set (dtype-proto/set-offset ba 5))))
    (is (= (vec (range 5))
           (vec (dtype/->reader ba)))))
  (let [big-range (seq (range 3000000000 3000000005))
        ba (bitmap/->bitmap big-range)]
    (is (= (vec big-range)
           (vec (dtype/->reader ba))))
    (is (= (set big-range)
           (set (dtype/->reader ba))))))
