(ns tech.v2.tensor.integration-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.tensor :as tens]
            [clojure.test :refer :all]))


(deftest tensor->array-and-back
  (let [test-tens (tens/->tensor (partition 3 (range 9)))]
    (doseq [row (tens/rows test-tens)]
      (is (= (tens/->jvm row)
             (vec (dtype/make-container :java-array :float32 row)))))

    (doseq [col (tens/columns test-tens)]
      (is (= (tens/->jvm col)
             (vec (dtype/make-container :java-array :float32 col)))))))


(deftest tensor->list-and-back
  (let [test-tens (tens/->tensor (partition 3 (range 9)))]
    (doseq [row (tens/rows test-tens)]
      (is (= (tens/->jvm row)
             (vec (dtype/make-container :list :float32 row)))))

    (doseq [col (tens/columns test-tens)]
      (is (= (tens/->jvm col)
             (vec (dtype/make-container :list :float32 col)))))))


(deftest block-rows->tensor
  (let [block-rows (repeatedly 4 #(int-array (range 5)))
        tensor (tens/new-tensor (dtype/shape block-rows)
                                :datatype :int32)]
    (dtype/copy-raw->item! block-rows tensor)
    (is (= [[0 1 2 3 4]
            [0 1 2 3 4]
            [0 1 2 3 4]
            [0 1 2 3 4]]
           (tens/->jvm tensor)))))
