(ns think.datatype.transpose-test
  (:require [think.datatype.core :as dtype]
            [think.datatype.transpose :refer [transpose]]
            [clojure.test :refer :all]
            [clojure.core.matrix :as m]))


(deftest interleaved->planar
  (let [n-channels 3
        height 16
        width 32
        answer (vec (mapcat #(repeat (* height width) %) (range 1 (+ 1 n-channels))))
        input (flatten (repeat (* height width) (range 1 (+ 1 n-channels))))
        guess (transpose (float-array input) [height width n-channels] [2 0 1])]
    (is (m/equals answer guess))))


(deftest planar->interleaved
  (let [n-channels 3
        height 16
        width 32
        input (vec (mapcat #(repeat (* height width) %) (range 1 (+ 1 n-channels))))
        answer (float-array (flatten (repeat (* height width) (range 1 (+ 1 n-channels)))))
        guess (transpose (float-array input) [n-channels height width] [1 2 0])]
    (is (m/equals answer guess))))
