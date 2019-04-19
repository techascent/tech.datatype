(ns tech.tensor.mmul-test
  (:require [tech.datatype :as dtype]
            [tech.tensor :as tens]
            [tech.tensor.select-test :refer [tensor-default-context]]
            [clojure.test :refer :all]))


(defn do-basic-mm
  [container-type datatype]
  (let [n-elems (* 2 5)
        test-mat (tens/->tensor (partition 5 (range n-elems))
                                :container-type container-type
                                :datatype datatype)]
    (is (= [[30 80] [80 255]]
           (-> (tens/matrix-multiply test-mat (tens/transpose test-mat [1 0]))
               (tens/->jvm :datatype :int32)))
        (with-out-str (println container-type datatype)))


    (is (= [[25 30 35 40 45]
            [30 37 44 51 58]
            [35 44 53 62 71]
            [40 51 62 73 84]
            [45 58 71 84 97]]
           (-> (tens/matrix-multiply (tens/transpose test-mat [1 0]) test-mat)
               (tens/->jvm :datatype :int32)))
        (with-out-str (println container-type datatype)))))


(deftest basic-mm
  (do-basic-mm :list :float64)
  (do-basic-mm :list :int32)
  (do-basic-mm :sparse :int32)
  (do-basic-mm :sparse :int16))
